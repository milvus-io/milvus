"""
CDC sync tests for RBAC operations.
"""

import time
from .base import TestCDCSyncBase, logger


class TestCDCSyncRBAC(TestCDCSyncBase):
    """Test CDC sync for RBAC operations."""

    def setup_method(self):
        """Setup for each test method."""
        self.resources_to_cleanup = []

    def teardown_method(self):
        """Cleanup after each test method - only cleanup upstream, downstream will sync."""
        upstream_client = getattr(self, "_upstream_client", None)

        if upstream_client:
            for resource_type, resource_name in self.resources_to_cleanup:
                if resource_type == "user":
                    self.cleanup_user(upstream_client, resource_name)
                elif resource_type == "role":
                    self.cleanup_role(upstream_client, resource_name)

            time.sleep(1)  # Allow cleanup to sync to downstream

    def test_create_role(self, upstream_client, downstream_client, sync_timeout):
        """Test CREATE_ROLE operation sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        role_name = self.gen_unique_name("test_role_create")
        self.resources_to_cleanup.append(("role", role_name))

        # Initial cleanup
        self.cleanup_role(upstream_client, role_name)

        # Create role in upstream
        upstream_client.create_role(role_name)
        assert role_name in upstream_client.list_roles()

        # Wait for sync to downstream
        def check_sync():
            return role_name in downstream_client.list_roles()

        assert self.wait_for_sync(check_sync, sync_timeout, f"create role {role_name}")

    def test_drop_role(self, upstream_client, downstream_client, sync_timeout):
        """Test DROP_ROLE operation sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        role_name = self.gen_unique_name("test_role_drop")
        self.resources_to_cleanup.append(("role", role_name))

        # Initial cleanup
        self.cleanup_role(upstream_client, role_name)

        # Create role first
        upstream_client.create_role(role_name)

        # Wait for creation to sync
        def check_create():
            return role_name in downstream_client.list_roles()

        assert self.wait_for_sync(
            check_create, sync_timeout, f"create role {role_name}"
        )

        # Drop role in upstream
        upstream_client.drop_role(role_name)
        assert role_name not in upstream_client.list_roles()

        # Wait for drop to sync
        def check_drop():
            return role_name not in downstream_client.list_roles()

        assert self.wait_for_sync(check_drop, sync_timeout, f"drop role {role_name}")

    def test_create_user(self, upstream_client, downstream_client, sync_timeout):
        """Test CREATE_USER operation sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        username = self.gen_unique_name("test_user_create", max_length=31)
        password = "TestPass123!"
        self.resources_to_cleanup.append(("user", username))

        # Initial cleanup
        self.cleanup_user(upstream_client, username)

        # Create user in upstream
        upstream_client.create_user(username, password)
        assert username in upstream_client.list_users()

        # Wait for sync to downstream
        def check_sync():
            return username in downstream_client.list_users()

        assert self.wait_for_sync(check_sync, sync_timeout, f"create user {username}")

    def test_drop_user(self, upstream_client, downstream_client, sync_timeout):
        """Test DROP_USER operation sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        username = self.gen_unique_name("test_user_drop", max_length=31)
        password = "TestPass123!"
        self.resources_to_cleanup.append(("user", username))

        # Initial cleanup
        self.cleanup_user(upstream_client, username)

        # Create user first
        upstream_client.create_user(username, password)

        # Wait for creation to sync
        def check_create():
            return username in downstream_client.list_users()

        assert self.wait_for_sync(check_create, sync_timeout, f"create user {username}")

        # Drop user in upstream
        upstream_client.drop_user(username)
        assert username not in upstream_client.list_users()

        # Wait for drop to sync
        def check_drop():
            return username not in downstream_client.list_users()

        assert self.wait_for_sync(check_drop, sync_timeout, f"drop user {username}")

    def test_grant_role(self, upstream_client, downstream_client, sync_timeout):
        """Test GRANT_ROLE operation sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        username = self.gen_unique_name("test_user_grant", max_length=31)
        role_name = self.gen_unique_name("test_role_grant")
        password = "TestPass123!"
        self.resources_to_cleanup.append(("user", username))
        self.resources_to_cleanup.append(("role", role_name))

        # Initial cleanup
        self.cleanup_user(upstream_client, username)
        self.cleanup_role(upstream_client, role_name)

        # Create user and role
        upstream_client.create_user(username, password)
        upstream_client.create_role(role_name)

        # Wait for creation to sync
        def check_create():
            return (
                username in downstream_client.list_users()
                and role_name in downstream_client.list_roles()
            )

        assert self.wait_for_sync(
            check_create, sync_timeout, "create user/role for grant"
        )

        # Grant role to user
        upstream_client.grant_role(username, role_name)

        # Wait for grant to sync
        def check_grant():
            # Allow operation to propagate
            time.sleep(2)
            # Verify role is bound to user using describe_user
            try:
                user_info = downstream_client.describe_user(username)
                user_roles = user_info.get("roles", [])
                print(f"DEBUG: user_roles in check_grant: {user_roles}")
                return role_name in user_roles
            except Exception as e:
                logger.debug(f"Error checking user roles: {e}")
                return False

        assert self.wait_for_sync(
            check_grant, sync_timeout, f"grant role {role_name} to user {username}"
        )

    def test_revoke_role(self, upstream_client, downstream_client, sync_timeout):
        """Test REVOKE_ROLE operation sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        username = self.gen_unique_name("test_user_revoke", max_length=31)
        role_name = self.gen_unique_name("test_role_revoke")
        password = "TestPass123!"
        self.resources_to_cleanup.append(("user", username))
        self.resources_to_cleanup.append(("role", role_name))

        # Initial cleanup
        self.cleanup_user(upstream_client, username)
        self.cleanup_role(upstream_client, role_name)

        # Create user and role, then grant role
        upstream_client.create_user(username, password)
        upstream_client.create_role(role_name)
        upstream_client.grant_role(username, role_name)

        # Wait for setup to sync
        time.sleep(5)

        # Revoke role from user
        upstream_client.revoke_role(username, role_name)

        # Wait for revoke to sync
        def check_revoke():
            time.sleep(2)  # Allow operation to propagate
            # Verify role is removed from user using describe_user
            try:
                user_info = downstream_client.describe_user(username)
                user_roles = user_info.get("roles", [])
                print(f"DEBUG: user_roles in check_revoke: {user_roles}")
                return role_name not in user_roles
            except Exception as e:
                logger.debug(f"Error checking user roles: {e}")
                return False

        assert self.wait_for_sync(
            check_revoke, sync_timeout, f"revoke role {role_name} from user {username}"
        )

    def test_grant_privilege(self, upstream_client, downstream_client, sync_timeout):
        """Test GRANT_PRIVILEGE operation sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        role_name = self.gen_unique_name("test_role_priv_grant")
        self.resources_to_cleanup.append(("role", role_name))

        # Initial cleanup
        self.cleanup_role(upstream_client, role_name)

        # Create role
        upstream_client.create_role(role_name)

        # Wait for creation to sync
        def check_create():
            return role_name in downstream_client.list_roles()

        assert self.wait_for_sync(
            check_create, sync_timeout, f"create role for privilege {role_name}"
        )

        # Grant privilege to role
        upstream_client.grant_privilege(
            role_name=role_name,
            object_type="Collection",
            privilege="Search",
            object_name="*",
        )

        # check privilege in upstream
        upstream_privilege = upstream_client.describe_role(role_name)
        print(f"DEBUG: upstream_privilege in check_grant: {upstream_privilege}")

        # Wait for privilege grant to sync
        def check_grant():
            time.sleep(2)
            # Verify privilege is actually granted using describe_role
            try:
                role_info = downstream_client.describe_role(role_name)
                print(f"DEBUG: role_info in check_grant: {role_info}")
                for privilege_info in role_info["privileges"]:
                    print(f"DEBUG: privilege_info in check_grant: {privilege_info}")
                    print(
                        f"DEBUG: privilege_info.get('object_type') in check_grant: {privilege_info.get('object_type')}"
                    )
                    print(
                        f"DEBUG: privilege_info.get('privilege') in check_grant: {privilege_info.get('privilege')}"
                    )
                    print(
                        f"DEBUG: privilege_info.get('object_name') in check_grant: {privilege_info.get('object_name')}"
                    )
                    if (
                        privilege_info.get("object_type") == "Collection"
                        and privilege_info.get("privilege") == "Search"
                        and privilege_info.get("object_name") == "*"
                    ):
                        return True
                return False
            except Exception as e:
                logger.debug(f"Error checking role privileges: {e}")
                return False

        assert self.wait_for_sync(
            check_grant, sync_timeout, f"grant privilege to role {role_name}"
        )

    def test_revoke_privilege(self, upstream_client, downstream_client, sync_timeout):
        """Test REVOKE_PRIVILEGE operation sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        role_name = self.gen_unique_name("test_role_priv_revoke")
        self.resources_to_cleanup.append(("role", role_name))

        # Initial cleanup
        self.cleanup_role(upstream_client, role_name)

        # Create role and grant privilege
        upstream_client.create_role(role_name)

        upstream_client.grant_privilege(
            role_name=role_name,
            object_type="Collection",
            privilege="Search",
            object_name="*",
        )

        # Wait for setup
        time.sleep(3)

        # Revoke privilege from role
        upstream_client.revoke_privilege(
            role_name=role_name,
            object_type="Collection",
            privilege="Search",
            object_name="*",
        )

        # Wait for revoke to sync
        def check_revoke():
            time.sleep(2)
            # Verify privilege is actually revoked using describe_role
            try:
                role_info = downstream_client.describe_role(role_name)
                for privilege_info in role_info["privileges"]:
                    if (
                        privilege_info.get("object_type") == "Collection"
                        and privilege_info.get("privilege") == "Search"
                        and privilege_info.get("object_name") == "*"
                    ):
                        return False  # Should not find the privilege
                return True
            except Exception as e:
                logger.debug(f"Error checking role privileges: {e}")
                return False

        assert self.wait_for_sync(
            check_revoke, sync_timeout, f"revoke privilege from role {role_name}"
        )

    def test_update_password(
        self, upstream_client, downstream_client, sync_timeout, downstream_uri
    ):
        """Test UPDATE_PASSWORD operation sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        username = self.gen_unique_name("test_user_pwd", max_length=31)
        old_password = "OldPass123!"
        new_password = "NewPass456!"
        self.resources_to_cleanup.append(("user", username))

        # Initial cleanup
        self.cleanup_user(upstream_client, username)

        # Create user first
        upstream_client.create_user(username, old_password)

        # Wait for creation to sync
        def check_create():
            return username in downstream_client.list_users()

        assert self.wait_for_sync(check_create, sync_timeout, f"create user {username}")

        # Update password in upstream
        upstream_client.update_password(username, old_password, new_password)

        # Wait for password update to sync
        def check_password_update():
            time.sleep(2)
            # Verify password update by trying to create a client with new credentials
            try:
                from pymilvus import MilvusClient

                # Try to connect with new password
                test_client = MilvusClient(
                    uri=downstream_uri, token=f"{username}:{new_password}"
                )
                # Simple operation to verify connection works
                test_client.list_collections()
                test_client.close()
                return True
            except Exception as e:
                logger.debug(f"Error verifying new password: {e}")
                return False

        assert self.wait_for_sync(
            check_password_update, sync_timeout, f"update password for user {username}"
        )

    def test_create_privilege_group(
        self, upstream_client, downstream_client, sync_timeout
    ):
        """Test CREATE_PRIVILEGE_GROUP operation sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        group_name = self.gen_unique_name("test_priv_group")
        # Note: privilege groups need special cleanup handling

        # Initial cleanup - try to drop if exists
        try:
            upstream_client.drop_privilege_group(group_name)
        except:
            pass  # Ignore if doesn't exist

        # Create privilege group in upstream
        upstream_client.create_privilege_group(group_name)

        # Verify in upstream
        def check_create():
            try:
                upstream_groups = upstream_client.list_privilege_groups()
                print(f"DEBUG: upstream_groups in check_create: {upstream_groups}")
                return group_name in [
                    group["privilege_group"] for group in upstream_groups
                ]
            except Exception as e:
                logger.debug(f"Error checking privilege groups: {e}")
                return False

        assert self.wait_for_sync(
            check_create, sync_timeout, f"create privilege group {group_name}"
        )

        # Wait for sync to downstream
        def check_sync():
            try:
                downstream_groups = downstream_client.list_privilege_groups()
                return group_name in [
                    group["privilege_group"] for group in downstream_groups
                ]
            except Exception as e:
                logger.debug(f"Error checking privilege groups: {e}")
                return False

        assert self.wait_for_sync(
            check_sync, sync_timeout, f"create privilege group {group_name}"
        )

        # Cleanup
        try:
            upstream_client.drop_privilege_group(group_name)
        except:
            pass

    def test_drop_privilege_group(
        self, upstream_client, downstream_client, sync_timeout
    ):
        """Test DROP_PRIVILEGE_GROUP operation sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        group_name = self.gen_unique_name("test_priv_group_drop")

        # Initial cleanup
        try:
            upstream_client.drop_privilege_group(group_name)
        except:
            pass

        # Create privilege group first
        upstream_client.create_privilege_group(group_name)

        # Wait for creation to sync
        def check_create():
            try:
                return group_name in [
                    group["privilege_group"]
                    for group in downstream_client.list_privilege_groups()
                ]
            except Exception as e:
                logger.debug(f"Error checking privilege groups: {e}")
                return False

        assert self.wait_for_sync(
            check_create, sync_timeout, f"create privilege group {group_name}"
        )

        # Drop privilege group in upstream
        upstream_client.drop_privilege_group(group_name)

        # Verify dropped in upstream
        upstream_groups = upstream_client.list_privilege_groups()
        assert group_name not in [group["privilege_group"] for group in upstream_groups]

        # Wait for drop to sync
        def check_drop():
            try:
                downstream_groups = downstream_client.list_privilege_groups()
                return group_name not in [
                    group["privilege_group"] for group in downstream_groups
                ]
            except Exception as e:
                logger.debug(f"Error checking privilege groups: {e}")
                return False

        assert self.wait_for_sync(
            check_drop, sync_timeout, f"drop privilege group {group_name}"
        )

    def test_grant_privilege_v2(self, upstream_client, downstream_client, sync_timeout):
        """Test GRANT_PRIVILEGE_V2 operation sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        role_name = self.gen_unique_name("test_role_priv_v2")
        self.resources_to_cleanup.append(("role", role_name))

        # Initial cleanup
        self.cleanup_role(upstream_client, role_name)

        # Create role
        upstream_client.create_role(role_name)

        # Wait for creation to sync
        def check_create():
            return role_name in downstream_client.list_roles()

        assert self.wait_for_sync(
            check_create, sync_timeout, f"create role for privilege v2 {role_name}"
        )

        # Grant privilege using v2 API
        upstream_client.grant_privilege_v2(
            role_name=role_name, privilege="Query", collection_name="*"
        )
        # check privilege in upstream
        upstream_privilege = upstream_client.describe_role(role_name)["privileges"]
        print(f"DEBUG: upstream_privilege in check_grant: {upstream_privilege}")

        # Wait for privilege grant to sync
        def check_grant():
            time.sleep(2)
            # Verify privilege is actually granted using describe_role
            try:
                role_info = downstream_client.describe_role(role_name)
                for privilege_info in role_info["privileges"]:
                    if (
                        privilege_info.get("privilege") == "Query"
                        and privilege_info.get("object_name") == "*"
                    ):
                        return True
                return False
            except Exception as e:
                logger.debug(f"Error checking role privileges v2: {e}")
                return False

        assert self.wait_for_sync(
            check_grant, sync_timeout, f"grant privilege v2 to role {role_name}"
        )

    def test_revoke_privilege_v2(
        self, upstream_client, downstream_client, sync_timeout
    ):
        """Test REVOKE_PRIVILEGE_V2 operation sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        role_name = self.gen_unique_name("test_role_revoke_v2")
        self.resources_to_cleanup.append(("role", role_name))

        # Initial cleanup
        self.cleanup_role(upstream_client, role_name)

        # Create role and grant privilege using v2 API
        upstream_client.create_role(role_name)
        upstream_client.grant_privilege_v2(
            role_name=role_name, privilege="Query", collection_name="*"
        )

        # Wait for setup
        time.sleep(3)

        # check privilege in upstream
        upstream_privilege = upstream_client.describe_role(role_name)["privileges"]
        print(f"DEBUG: upstream_privilege in check_revoke: {upstream_privilege}")

        # check privilege in downstream
        def check_grant():
            try:
                role_info = downstream_client.describe_role(role_name)
                for privilege_info in role_info["privileges"]:
                    if (
                        privilege_info.get("privilege") == "Query"
                        and privilege_info.get("object_name") == "*"
                    ):
                        return True
                return False
            except Exception as e:
                logger.debug(f"Error checking role privileges v2: {e}")
                return False

        assert self.wait_for_sync(
            check_grant, sync_timeout, f"grant privilege v2 to role {role_name}"
        )

        # Revoke privilege using v2 API
        upstream_client.revoke_privilege_v2(
            role_name=role_name, privilege="Query", collection_name="*"
        )
        # check privilege in upstream
        upstream_privilege = upstream_client.describe_role(role_name)["privileges"]
        print(f"DEBUG: upstream_privilege in check_revoke: {upstream_privilege}")

        # Wait for revoke to sync
        def check_revoke():
            time.sleep(2)
            # Verify privilege is actually revoked using describe_role
            try:
                role_info = downstream_client.describe_role(role_name)
                for privilege_info in role_info["privileges"]:
                    if (
                        privilege_info.get("privilege") == "Query"
                        and privilege_info.get("object_name") == "*"
                    ):
                        return False  # Should not find the privilege
                return True
            except Exception as e:
                logger.debug(f"Error checking role privileges v2: {e}")
                return False

        assert self.wait_for_sync(
            check_revoke, sync_timeout, f"revoke privilege v2 from role {role_name}"
        )

    def test_add_privileges_to_group(
        self, upstream_client, downstream_client, sync_timeout
    ):
        """Test ADD_PRIVILEGES_TO_GROUP operation sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        group_name = self.gen_unique_name("test_priv_group_add")

        # Initial cleanup
        try:
            upstream_client.drop_privilege_group(group_name)
        except:
            pass

        # Create privilege group first
        upstream_client.create_privilege_group(group_name)

        # Wait for creation to sync
        def check_create():
            try:
                downstream_groups = downstream_client.list_privilege_groups()
                print(f"DEBUG: downstream_groups in check_create: {downstream_groups}")
                return group_name in [
                    group["privilege_group"] for group in downstream_groups
                ]
            except Exception as e:
                logger.debug(f"Error checking privilege groups: {e}")
                return False

        assert self.wait_for_sync(
            check_create, sync_timeout, f"create privilege group {group_name}"
        )

        # Add privileges to group in upstream
        privileges_to_add = ["Search", "Query"]
        upstream_client.add_privileges_to_group(group_name, privileges_to_add)

        # Wait for add privileges to sync
        def check_upstream():
            client = upstream_client
            try:
                groups = client.list_privilege_groups()
                print(f"DEBUG: groups in check_add: {groups}")
                assert group_name in [group["privilege_group"] for group in groups]
                group_info = None
                for group in groups:
                    if group["privilege_group"] == group_name:
                        group_info = group
                        break
                assert group_info is not None
                assert "Search" in group_info["privileges"]
                assert "Query" in group_info["privileges"]
                return True

            except Exception as e:
                logger.debug(f"Error checking privilege groups: {e}")
                return False

        assert self.wait_for_sync(
            check_upstream, sync_timeout, f"add privileges to group {group_name}"
        )

        # Wait for add privileges to sync
        def check_downstream():
            client = upstream_client
            try:
                groups = client.list_privilege_groups()
                print(f"DEBUG: groups in check_add: {groups}")
                assert group_name in [group["privilege_group"] for group in groups]
                group_info = None
                for group in groups:
                    if group["privilege_group"] == group_name:
                        group_info = group
                        break
                assert group_info is not None
                assert "Search" in group_info["privileges"]
                assert "Query" in group_info["privileges"]
                return True

            except Exception as e:
                logger.debug(f"Error checking privilege groups: {e}")
                return False

        assert self.wait_for_sync(
            check_downstream, sync_timeout, f"add privileges to group {group_name}"
        )

        # Cleanup
        try:
            upstream_client.drop_privilege_group(group_name)
        except:
            pass

    def test_remove_privileges_from_group(
        self, upstream_client, downstream_client, sync_timeout
    ):
        """Test REMOVE_PRIVILEGES_FROM_GROUP operation sync."""
        # Store upstream client for teardown
        self._upstream_client = upstream_client

        group_name = self.gen_unique_name("test_priv_group_add")

        # Initial cleanup
        try:
            upstream_client.drop_privilege_group(group_name)
        except:
            pass

        # Create privilege group first
        upstream_client.create_privilege_group(group_name)

        # Wait for creation to sync
        def check_create():
            try:
                downstream_groups = downstream_client.list_privilege_groups()
                print(f"DEBUG: downstream_groups in check_create: {downstream_groups}")
                return group_name in [
                    group["privilege_group"] for group in downstream_groups
                ]
            except Exception as e:
                logger.debug(f"Error checking privilege groups: {e}")
                return False

        assert self.wait_for_sync(
            check_create, sync_timeout, f"create privilege group {group_name}"
        )

        # Add privileges to group in upstream
        privileges_to_add = ["Search", "Query"]
        upstream_client.add_privileges_to_group(group_name, privileges_to_add)

        # Wait for add privileges to sync
        def check_upstream():
            client = upstream_client
            try:
                groups = client.list_privilege_groups()
                print(f"DEBUG: groups in check_add: {groups}")
                assert group_name in [group["privilege_group"] for group in groups]
                group_info = None
                for group in groups:
                    if group["privilege_group"] == group_name:
                        group_info = group
                        break
                assert group_info is not None
                assert "Search" in group_info["privileges"]
                assert "Query" in group_info["privileges"]
                return True

            except Exception as e:
                logger.debug(f"Error checking privilege groups: {e}")
                return False

        assert self.wait_for_sync(
            check_upstream, sync_timeout, f"add privileges to group {group_name}"
        )

        # Wait for add privileges to sync
        def check_downstream():
            client = upstream_client
            try:
                groups = client.list_privilege_groups()
                print(f"DEBUG: groups in check_add: {groups}")
                assert group_name in [group["privilege_group"] for group in groups]
                group_info = None
                for group in groups:
                    if group["privilege_group"] == group_name:
                        group_info = group
                        break
                assert group_info is not None
                assert "Search" in group_info["privileges"]
                assert "Query" in group_info["privileges"]
                return True

            except Exception as e:
                logger.debug(f"Error checking privilege groups: {e}")
                return False

        assert self.wait_for_sync(
            check_downstream, sync_timeout, f"add privileges to group {group_name}"
        )

        # remove privileges from group in upstream
        privileges_to_remove = ["Search", "Query"]
        upstream_client.remove_privileges_from_group(group_name, privileges_to_remove)

        # Wait for remove privileges to sync
        def check_upstream():
            client = upstream_client
            try:
                groups = client.list_privilege_groups()
                print(f"DEBUG: groups in check_remove: {groups}")
                assert group_name in [group["privilege_group"] for group in groups]
                group_info = None
                for group in groups:
                    if group["privilege_group"] == group_name:
                        group_info = group
                        break
                assert group_info is not None
                assert "Search" not in group_info["privileges"]
                assert "Query" not in group_info["privileges"]
                return True

            except Exception as e:
                logger.debug(f"Error checking privilege groups: {e}")
                return False

        assert self.wait_for_sync(
            check_upstream, sync_timeout, f"remove privileges from group {group_name}"
        )

        # Wait for remove privileges to sync
        def check_downstream():
            client = downstream_client
            try:
                groups = client.list_privilege_groups()
                print(f"DEBUG: groups in check_remove: {groups}")
                assert group_name in [group["privilege_group"] for group in groups]
                group_info = None
                for group in groups:
                    if group["privilege_group"] == group_name:
                        group_info = group
                        break
                assert group_info is not None
                assert "Search" not in group_info["privileges"]
                assert "Query" not in group_info["privileges"]
                return True

            except Exception as e:
                logger.debug(f"Error checking privilege groups: {e}")
                return False

        assert self.wait_for_sync(
            check_downstream, sync_timeout, f"remove privileges from group {group_name}"
        )
