# RBAC Messages

Messages managing users, roles, privileges, and privilege groups. All are CChannel-only broadcasts serialized via ExclusivePrivilege resource key.

All broadcast messages implicitly carry **SharedCluster** via the Broadcaster.

| Message | Dispatch | ExclusiveRequired | ResourceKey |
|---------|----------|-------------------|-------------|
| AlterUser | Broadcast: CChannel | No | ExclusivePrivilege |
| DropUser | Broadcast: CChannel | No | ExclusivePrivilege |
| AlterRole | Broadcast: CChannel | No | ExclusivePrivilege |
| DropRole | Broadcast: CChannel | No | ExclusivePrivilege |
| AlterUserRole | Broadcast: CChannel | No | ExclusivePrivilege |
| DropUserRole | Broadcast: CChannel | No | ExclusivePrivilege |
| AlterPrivilege | Broadcast: CChannel | No | ExclusivePrivilege |
| DropPrivilege | Broadcast: CChannel | No | ExclusivePrivilege |
| AlterPrivilegeGroup | Broadcast: CChannel | No | ExclusivePrivilege |
| DropPrivilegeGroup | Broadcast: CChannel | No | ExclusivePrivilege |
| RestoreRBAC | Broadcast: CChannel | No | ExclusivePrivilege |

- **AlterUser**: Creates or updates a user credential.
- **DropUser**: Deletes a user and cleans up associated role bindings.
- **AlterRole**: Creates a new role.
- **DropRole**: Deletes a role, its grants, and refreshes proxy policy caches.
- **AlterUserRole**: Binds a user to a role.
- **DropUserRole**: Unbinds a user from a role.
- **AlterPrivilege**: Grants a privilege to a role on a specific resource.
- **DropPrivilege**: Revokes a privilege from a role on a specific resource.
- **AlterPrivilegeGroup**: Creates a privilege group or adds privileges to an existing group.
- **DropPrivilegeGroup**: Drops a privilege group or removes privileges from an existing group.
- **RestoreRBAC**: Bulk-restores RBAC metadata from a backup.

## Key Invariants

All RBAC messages are mutually serialized via ExclusivePrivilege resource key. No two RBAC operations can be in flight concurrently.
