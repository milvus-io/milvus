import re
import time
import numpy as np
import pytest

from base.client_v2_base import TestMilvusClientV2Base
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks

prefix = "rbac_entity"
user_pre = "user"
role_pre = "role"
default_nb = 100
default_dim = ct.default_dim
default_primary_key_field_name = "id"
default_vector_field_name = "vector"
default_float_field_name = ct.default_float_field_name
default_string_field_name = ct.default_string_field_name

# How long to wait for privilege cache to refresh (seconds)
PRIVILEGE_CACHE_REFRESH = 10

# How long to wait for Milvus to hot-reload yaml config changes (seconds)
CONFIG_RELOAD_WAIT = 5


def gen_rows(nb=default_nb, dim=default_dim):
    """Generate test data rows."""
    rng = np.random.default_rng(seed=19530)
    return [{
        default_primary_key_field_name: i,
        default_vector_field_name: list(rng.random((1, dim))[0]),
        default_float_field_name: i * 1.0,
        default_string_field_name: str(i),
    } for i in range(nb)]


def _set_authorization_enabled(config_path, enabled):
    """Toggle authorizationEnabled in milvus.yaml via text replacement."""
    with open(config_path) as f:
        content = f.read()
    target = "true" if enabled else "false"
    content = re.sub(
        r"(authorizationEnabled:\s*)(?:true|false)",
        rf"\g<1>{target}",
        content,
    )
    with open(config_path, "w") as f:
        f.write(content)


@pytest.mark.tags(CaseLabel.RBAC)
class TestRbacEntityBased(TestMilvusClientV2Base):
    """
    E2E tests for RBAC entity-based (ID-based) authorization.

    Verifies that grants follow the collection entity (by ID) rather than the name,
    covering: rename, drop+recreate, alias, alias redirection, and wildcards.

    Pass --milvus_config <path-to-milvus.yaml> to auto-enable authorization
    for the duration of these tests.  Milvus hot-reloads the config, so no
    restart is needed.  If --milvus_config is not provided, the tests assume
    authorization is already enabled on the server.

    Ref: docs/plans/2026-03-04-rbac-entity-based-authorization.md §Verification
    """

    @pytest.fixture(scope="class", autouse=True)
    def enable_authorization(self, request):
        """Enable authorizationEnabled in milvus.yaml for this test class."""
        config_path = request.config.getoption("--milvus_config")
        if config_path:
            log.info(f"[fixture] enabling authorizationEnabled via {config_path}")
            _set_authorization_enabled(config_path, True)
            time.sleep(CONFIG_RELOAD_WAIT)
            yield
            log.info(f"[fixture] restoring authorizationEnabled=false via {config_path}")
            _set_authorization_enabled(config_path, False)
            time.sleep(CONFIG_RELOAD_WAIT)
        else:
            yield

    def teardown_method(self, method):
        """Clean up users, roles, aliases, collections after each test."""
        log.info("[teardown] cleaning up RBAC test resources ...")
        client = self._client()

        # 1. Drop collections first — this triggers async DeleteGrantByCollectionID
        collections, _ = self.list_collections(client)
        for col in collections:
            try:
                self.drop_collection(client, col)
            except Exception as e:
                log.warning(f"[teardown] failed to drop collection {col}: {e}")

        # 2. Wait for async grant cleanup callbacks to finish
        time.sleep(3)

        # 3. Drop non-default users (removes role bindings)
        users, _ = self.list_users(client)
        for user in users:
            if user != ct.default_user:
                try:
                    self.drop_user(client, user)
                except Exception as e:
                    log.warning(f"[teardown] failed to drop user {user}: {e}")

        # 4. Revoke remaining privileges and drop non-builtin roles
        roles, _ = self.list_roles(client)
        for role in roles:
            if role not in ["admin", "public"]:
                try:
                    res, _ = self.describe_role(client, role)
                    if res and res.get("privileges"):
                        for priv in res["privileges"]:
                            self.revoke_privilege(
                                client, role, priv["object_type"],
                                priv["privilege"], priv["object_name"],
                                db_name=priv.get("db_name", ""),
                            )
                    self.drop_role(client, role)
                except Exception as e:
                    log.warning(f"[teardown] failed to drop role {role}: {e}")

        super().teardown_method(method)

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    def _setup_user_with_role(self, client_root, host, port):
        """Create a user + role, bind them, return (user_client, role_name)."""
        user_name = cf.gen_unique_str(user_pre)
        role_name = cf.gen_unique_str(role_pre)
        password = cf.gen_str_by_length(contain_numbers=True)

        self.create_user(client_root, user_name=user_name, password=password)
        self.create_role(client_root, role_name=role_name)
        self.grant_role(client_root, user_name=user_name, role_name=role_name)

        uri = f"http://{host}:{port}"
        user_client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)
        return user_client, role_name

    def _create_collection_with_data(self, client, name, nb=default_nb):
        """Create a collection and insert test data."""
        self.create_collection(client, name, default_dim, consistency_level="Strong")
        rows = gen_rows(nb)
        self.insert(client, name, rows)
        self.flush(client, name)

    def _search(self, client, collection_name, check_task=None, check_items=None):
        """Perform a search on the collection."""
        rng = np.random.default_rng(seed=19530)
        vectors = list(rng.random((1, default_dim)))
        return self.search(
            client, collection_name, vectors, limit=10,
            check_task=check_task,
            check_items=check_items,
        )

    # ------------------------------------------------------------------
    # 1. Rename safety
    # ------------------------------------------------------------------
    def test_rbac_rename_safety(self, host, port):
        """
        Verify: Grant on colA → rename to colB → access colB → permit (ID unchanged).

        After rename, the collection ID stays the same, so the grant should still
        apply when accessing the collection via its new name.
        """
        client_root = self._client()
        col_a = cf.gen_unique_str(prefix)
        col_b = cf.gen_unique_str(prefix)

        # Create collection and insert data
        self._create_collection_with_data(client_root, col_a)

        # Setup user with role
        user_client, role_name = self._setup_user_with_role(client_root, host, port)

        # Grant Search privilege on colA
        self.grant_privilege(client_root, role_name, "Collection", "Search", col_a)
        time.sleep(PRIVILEGE_CACHE_REFRESH)

        # Verify user can search colA
        self._search(user_client, col_a)

        # Rename colA → colB
        self.rename_collection(client_root, col_a, col_b)
        time.sleep(PRIVILEGE_CACHE_REFRESH)

        # User should still be able to search via new name colB (same entity)
        self._search(user_client, col_b)

        # Accessing old name colA should fail (collection no longer exists under that name)
        self._search(user_client, col_a,
                     check_task=CheckTasks.err_res,
                     check_items={ct.err_code: 100, ct.err_msg: "collection not found"})

        # Cleanup
        self.drop_collection(client_root, col_b)

    # ------------------------------------------------------------------
    # 2. Drop+Recreate safety
    # ------------------------------------------------------------------
    def test_rbac_drop_recreate_safety(self, host, port):
        """
        Verify: Grant on colA → drop → recreate colA → deny (new ID).

        After drop+recreate, the collection gets a new ID, so the old grant should
        NOT apply to the new collection — preventing permission leakage.
        """
        client_root = self._client()
        col_name = cf.gen_unique_str(prefix)

        # Create collection and insert data
        self._create_collection_with_data(client_root, col_name)

        # Setup user with role
        user_client, role_name = self._setup_user_with_role(client_root, host, port)

        # Grant Search privilege on the collection
        self.grant_privilege(client_root, role_name, "Collection", "Search", col_name)
        time.sleep(PRIVILEGE_CACHE_REFRESH)

        # Verify user can search
        self._search(user_client, col_name)

        # Drop the collection
        self.drop_collection(client_root, col_name)

        # Recreate with the same name (gets a new ID)
        self._create_collection_with_data(client_root, col_name)
        time.sleep(PRIVILEGE_CACHE_REFRESH)

        # User should NOT be able to search the recreated collection (different entity)
        self._search(user_client, col_name, check_task=CheckTasks.check_permission_deny)

        # Cleanup
        self.drop_collection(client_root, col_name)

    # ------------------------------------------------------------------
    # 3. Alias access
    # ------------------------------------------------------------------
    def test_rbac_alias_access(self, host, port):
        """
        Verify: Grant on colA → access via alias → permit (alias → same ID).

        When a user has permission on the real collection, accessing it through
        an alias should succeed because alias resolves to the same entity.
        """
        client_root = self._client()
        col_name = cf.gen_unique_str(prefix)
        alias_name = cf.gen_unique_str("alias")

        # Create collection with data and alias
        self._create_collection_with_data(client_root, col_name)
        self.create_alias(client_root, col_name, alias_name)

        # Setup user with role
        user_client, role_name = self._setup_user_with_role(client_root, host, port)

        # Grant Search privilege on real collection name
        self.grant_privilege(client_root, role_name, "Collection", "Search", col_name)
        time.sleep(PRIVILEGE_CACHE_REFRESH)

        # User should be able to search via real name
        self._search(user_client, col_name)

        # User should also be able to search via alias (resolves to same ID)
        self._search(user_client, alias_name)

        # Cleanup
        self.drop_alias(client_root, alias_name)
        self.drop_collection(client_root, col_name)

    # ------------------------------------------------------------------
    # 3b. Alias grant access
    # ------------------------------------------------------------------
    def test_rbac_alias_grant_access(self, host, port):
        """
        Verify: Grant on alias → access via real name → permit.

        When permission is granted using an alias, accessing the collection via
        the real name should also succeed because grant resolves alias to entity ID.
        """
        client_root = self._client()
        col_name = cf.gen_unique_str(prefix)
        alias_name = cf.gen_unique_str("alias")

        # Create collection with data and alias
        self._create_collection_with_data(client_root, col_name)
        self.create_alias(client_root, col_name, alias_name)

        # Setup user with role
        user_client, role_name = self._setup_user_with_role(client_root, host, port)

        # Grant Search privilege using ALIAS name
        self.grant_privilege(client_root, role_name, "Collection", "Search", alias_name)
        time.sleep(PRIVILEGE_CACHE_REFRESH)

        # User should be able to search via alias
        self._search(user_client, alias_name)

        # User should also be able to search via real name (same entity ID)
        self._search(user_client, col_name)

        # Cleanup
        self.drop_alias(client_root, alias_name)
        self.drop_collection(client_root, col_name)

    # ------------------------------------------------------------------
    # 4. Alias redirection
    # ------------------------------------------------------------------
    def test_rbac_alias_redirection(self, host, port):
        """
        Verify: Grant on aliasA(→colA) → alter alias to colB → access via aliasA → deny colB.

        When an alias is redirected to a different collection, the grant should NOT
        follow the alias to the new collection — the grant is on the entity (colA),
        not the alias pointer.
        """
        client_root = self._client()
        col_a = cf.gen_unique_str(prefix)
        col_b = cf.gen_unique_str(prefix)
        alias_name = cf.gen_unique_str("alias")

        # Create two collections with data
        self._create_collection_with_data(client_root, col_a)
        self._create_collection_with_data(client_root, col_b)

        # Create alias pointing to colA
        self.create_alias(client_root, col_a, alias_name)

        # Setup user with role
        user_client, role_name = self._setup_user_with_role(client_root, host, port)

        # Grant Search privilege on colA (through alias or direct name)
        self.grant_privilege(client_root, role_name, "Collection", "Search", col_a)
        time.sleep(PRIVILEGE_CACHE_REFRESH)

        # User can search colA via alias
        self._search(user_client, alias_name)

        # Redirect alias to colB
        self.alter_alias(client_root, col_b, alias_name)
        time.sleep(PRIVILEGE_CACHE_REFRESH)

        # User should NOT be able to search colB via alias (no grant on colB)
        self._search(user_client, alias_name, check_task=CheckTasks.check_permission_deny)

        # User should still be able to search colA directly
        self._search(user_client, col_a)

        # Cleanup
        self.drop_alias(client_root, alias_name)
        self.drop_collection(client_root, col_a)
        self.drop_collection(client_root, col_b)

    # ------------------------------------------------------------------
    # 5. Wildcard grant
    # ------------------------------------------------------------------
    def test_rbac_wildcard_grant(self, host, port):
        """
        Verify: Grant * → access any collection → permit.

        A wildcard grant on all collections should work regardless of whether
        the internal storage uses ID-based or name-based keys.
        """
        client_root = self._client()
        col_name = cf.gen_unique_str(prefix)

        # Create collection with data
        self._create_collection_with_data(client_root, col_name)

        # Setup user with role
        user_client, role_name = self._setup_user_with_role(client_root, host, port)

        # Grant Search privilege on ALL collections (wildcard)
        self.grant_privilege(client_root, role_name, "Collection", "Search", "*")
        time.sleep(PRIVILEGE_CACHE_REFRESH)

        # User should be able to search any collection
        self._search(user_client, col_name)

        # Create another collection — wildcard should cover it too
        col_name2 = cf.gen_unique_str(prefix)
        self._create_collection_with_data(client_root, col_name2)
        self._search(user_client, col_name2)

        # Cleanup
        self.drop_collection(client_root, col_name)
        self.drop_collection(client_root, col_name2)

    # ------------------------------------------------------------------
    # 6. Rename + alias combined
    # ------------------------------------------------------------------
    def test_rbac_rename_then_alias(self, host, port):
        """
        Verify rename safety combined with alias resolution.

        Grant on colA → rename to colB → create alias on colB → access via alias → permit.
        """
        client_root = self._client()
        col_a = cf.gen_unique_str(prefix)
        col_b = cf.gen_unique_str(prefix)
        alias_name = cf.gen_unique_str("alias")

        # Create collection and insert data
        self._create_collection_with_data(client_root, col_a)

        # Setup user with role
        user_client, role_name = self._setup_user_with_role(client_root, host, port)

        # Grant Search on colA
        self.grant_privilege(client_root, role_name, "Collection", "Search", col_a)
        time.sleep(PRIVILEGE_CACHE_REFRESH)

        # Rename colA → colB
        self.rename_collection(client_root, col_a, col_b)

        # Create alias on colB
        self.create_alias(client_root, col_b, alias_name)
        time.sleep(PRIVILEGE_CACHE_REFRESH)

        # User should be able to search via alias (resolves to colB → same entity)
        self._search(user_client, alias_name)

        # User should also be able to search via new name colB
        self._search(user_client, col_b)

        # Cleanup
        self.drop_alias(client_root, alias_name)
        self.drop_collection(client_root, col_b)

    # ------------------------------------------------------------------
    # 7. Revoke after rename
    # ------------------------------------------------------------------
    def test_rbac_revoke_after_rename(self, host, port):
        """
        Verify: Grant on colA → rename to colB → revoke on colB → deny.

        Revoke should work on the renamed collection because it targets the same entity.
        """
        client_root = self._client()
        col_a = cf.gen_unique_str(prefix)
        col_b = cf.gen_unique_str(prefix)

        # Create collection and insert data
        self._create_collection_with_data(client_root, col_a)

        # Setup user with role
        user_client, role_name = self._setup_user_with_role(client_root, host, port)

        # Grant Search on colA
        self.grant_privilege(client_root, role_name, "Collection", "Search", col_a)
        time.sleep(PRIVILEGE_CACHE_REFRESH)

        # Verify search works
        self._search(user_client, col_a)

        # Rename colA → colB
        self.rename_collection(client_root, col_a, col_b)
        time.sleep(PRIVILEGE_CACHE_REFRESH)

        # Revoke Search on colB (which is the same entity)
        self.revoke_privilege(client_root, role_name, "Collection", "Search", col_b)
        time.sleep(PRIVILEGE_CACHE_REFRESH)

        # User should NOT be able to search colB anymore
        self._search(user_client, col_b, check_task=CheckTasks.check_permission_deny)

        # Cleanup
        self.drop_collection(client_root, col_b)

    # ------------------------------------------------------------------
    # 8. Grant on non-existent collection should fail
    # ------------------------------------------------------------------
    def test_rbac_grant_nonexistent_collection(self, host, port):
        """
        Verify: Granting privileges on a non-existent collection returns error.

        After migrating to ID-based grants, the system must resolve collection name
        to ID during grant. If the collection doesn't exist, grant should fail.
        """
        client_root = self._client()
        role_name = cf.gen_unique_str(role_pre)
        self.create_role(client_root, role_name=role_name)

        non_existent = cf.gen_unique_str("nonexistent")

        # Grant on a collection that doesn't exist should fail
        self.grant_privilege(
            client_root, role_name, "Collection", "Search", non_existent,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 100, ct.err_msg: "collection not found"},
        )

    # ------------------------------------------------------------------
    # 9. Multiple privileges survive rename
    # ------------------------------------------------------------------
    def test_rbac_multiple_privileges_rename(self, host, port):
        """
        Verify: Multiple grants on colA → rename to colB → all privileges still work.
        """
        client_root = self._client()
        col_a = cf.gen_unique_str(prefix)
        col_b = cf.gen_unique_str(prefix)

        # Create collection and insert data
        self._create_collection_with_data(client_root, col_a)

        # Setup user with role
        user_client, role_name = self._setup_user_with_role(client_root, host, port)

        # Grant multiple privileges on colA
        self.grant_privilege(client_root, role_name, "Collection", "Search", col_a)
        self.grant_privilege(client_root, role_name, "Collection", "Insert", col_a)
        self.grant_privilege(client_root, role_name, "Collection", "Query", col_a)
        time.sleep(PRIVILEGE_CACHE_REFRESH)

        # Rename
        self.rename_collection(client_root, col_a, col_b)
        time.sleep(PRIVILEGE_CACHE_REFRESH)

        # All privileges should still work on colB
        self._search(user_client, col_b)

        rows = gen_rows(10)
        self.insert(user_client, col_b, rows)

        self.query(user_client, col_b, filter="id >= 0", limit=10)

        # Cleanup
        self.drop_collection(client_root, col_b)

    # ------------------------------------------------------------------
    # 10. Drop recreate does not leak insert privilege
    # ------------------------------------------------------------------
    def test_rbac_drop_recreate_insert_no_leak(self, host, port):
        """
        Verify: Grant Insert on colA → drop → recreate colA → deny Insert.

        Tests that write privileges (not just read) are also properly cleaned up.
        """
        client_root = self._client()
        col_name = cf.gen_unique_str(prefix)

        # Create collection
        self._create_collection_with_data(client_root, col_name)

        # Setup user with role
        user_client, role_name = self._setup_user_with_role(client_root, host, port)

        # Grant Insert privilege
        self.grant_privilege(client_root, role_name, "Collection", "Insert", col_name)
        time.sleep(PRIVILEGE_CACHE_REFRESH)

        # Verify insert works
        rows = gen_rows(10)
        self.insert(user_client, col_name, rows)

        # Drop and recreate
        self.drop_collection(client_root, col_name)
        self._create_collection_with_data(client_root, col_name)
        time.sleep(PRIVILEGE_CACHE_REFRESH)

        # Insert should be denied on the new collection
        self.insert(user_client, col_name, rows,
                    check_task=CheckTasks.check_permission_deny)

        # Cleanup
        self.drop_collection(client_root, col_name)

    # ------------------------------------------------------------------
    # 11. Wildcard grant survives rename
    # ------------------------------------------------------------------
    def test_rbac_wildcard_grant_survives_rename(self, host, port):
        """
        Verify: Wildcard grant * → rename colA to colB → user can still search colB.

        Wildcard grants match all collections regardless of name changes.
        """
        client_root = self._client()
        col_a = cf.gen_unique_str(prefix)
        col_b = cf.gen_unique_str(prefix)

        self._create_collection_with_data(client_root, col_a)

        user_client, role_name = self._setup_user_with_role(client_root, host, port)

        # Grant wildcard
        self.grant_privilege(client_root, role_name, "Collection", "Search", "*")
        time.sleep(PRIVILEGE_CACHE_REFRESH)

        self._search(user_client, col_a)

        # Rename
        self.rename_collection(client_root, col_a, col_b)
        time.sleep(PRIVILEGE_CACHE_REFRESH)

        # Should still work via wildcard
        self._search(user_client, col_b)

        # Cleanup
        self.drop_collection(client_root, col_b)
