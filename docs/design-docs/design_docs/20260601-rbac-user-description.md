# MEP: RBAC User Description

- **Created:** 2026-06-01
- **Author(s):** @shaoting-huang
- **Status:** Under Review
- **Component:** Proxy | Coordinator
- **Related Issues:** #50179
- **Released:** Milvus release version, if applicable

## Summary

Milvus RBAC users can carry an optional human-readable description. The field is
accepted on user creation, returned by user read APIs, and can be edited through
the existing credential update API without requiring a password change.

**GitHub Issue**: https://github.com/milvus-io/milvus/issues/50179

## Motivation

RBAC users are currently identified only by username and role bindings. Operators
need a lightweight place to record who owns a user, what integration it belongs
to, or why it exists. The description must be editable without rotating the
password, and password rotation must not erase the description.

## Goals

- Persist a user description together with credential metadata.
- Return the description from user describe and select flows, including when
  role information is not requested.
- Allow description-only updates through `UpdateCredential`.
- Preserve the existing password when only the description changes.
- Preserve the existing description when only the password changes.
- Avoid invalidating or blanking proxy authentication cache entries during
  description-only updates.

## Non-Goals

- Add a new RPC for user description edits.
- Change RBAC authorization semantics. A caller with `PrivilegeUpdateUser` can
  update a description without knowing the target user's password.
- Add user descriptions to RBAC backup and restore. The current proto dependency
  does not add description to `UserInfo`.

## Public Interfaces

The milvus-proto dependency adds optional description fields to the existing
credential requests and user read result:

```protobuf
message CreateCredentialRequest {
  optional string description = 6;
}

message UpdateCredentialRequest {
  optional string description = 7;
}

message UserResult {
  UserEntity user = 1;
  repeated RoleEntity roles = 2;
  string description = 3;
}
```

Milvus internal credential messages add the same optional field so the WAL body
can distinguish "field not provided" from "set description to empty string":

```protobuf
message CredentialInfo {
  string username = 1;
  string encrypted_password = 2;
  string sha256_password = 5;
  uint64 time_tick = 6;
optional string description = 7;
}
```

`proxy.maxUserDescriptionLength` limits the byte length of the description. The
default is 1024 bytes.

## Design Details

### Data Flow

#### Create User

1. Proxy validates username, password, and description length.
2. Proxy encrypts the password, computes the SHA256 cache value, and sends
   `CredentialInfo` with `description` to RootCoord.
3. RootCoord broadcasts an alter-user WAL message.
4. The WAL ack callback writes the credential metadata and updates proxy auth
   caches because a password is present.

#### Update Password

1. Proxy enters the password update path only when `new_password` is provided.
2. Proxy decodes and validates the old and new passwords, verifies the old
   password unless the caller is a configured super user, then sends the new
   encrypted password and SHA256 cache value.
3. RootCoord performs a read-modify-write merge. If the incoming message does
   not carry `description`, the existing description is preserved.
4. The WAL ack callback updates proxy auth caches because the body carries a
   non-empty SHA256 password.

#### Update Description Only

1. Proxy validates description length and skips the password block because
   `new_password` is absent.
2. Proxy sends `CredentialInfo` with `description` and no password fields.
3. RootCoord merges the incoming metadata with the existing credential. Because
   the incoming encrypted password is empty, the existing encrypted password is
   preserved.
4. The WAL ack callback skips proxy auth cache updates because no SHA256
   password is present.

#### Read User

`Catalog.getUserResult` loads the credential before the role-info early return
and copies `Credential.Description` into `UserResult.Description`. This makes
both `include_role_info=true` and `include_role_info=false` return the field.

## Storage Model

`model.Credential` stores `Description` in the same JSON payload as the encrypted
password and timetick. `Sha256Password` remains cache-only and is not persisted
by the RootCoord merge path.

Proxy enforces the description length before the request reaches RootCoord, so an
oversized description is rejected before it can increase the etcd credential
value. RootCoord keeps the existing credential validation shape and does not
duplicate proxy-side username, password, or description length checks.

HTTP v2 user create and update requests pass the optional description through to
the same gRPC credential APIs. HTTP v2 user describe preserves the existing role
list response and includes the description in the response object.

## Compatibility, Deprecation, and Migration Plan

The description field is optional. Existing credential records unmarshal with an
empty description. Existing clients that do not send descriptions continue to
create and update credentials as before.

This Milvus PR temporarily uses a git-based `replace` for the milvus-proto
branch that defines the API fields. During coordinated landing, the replace is
removed and the dependency is switched to the upstream milvus-proto version that
contains those fields.

No deprecation or data migration is required.

## Test Plan

- Proxy unit tests cover create with description, description length rejection,
  description-only update, password-plus-description update, empty update
  rejection, and empty-string description clearing.
- RootCoord unit tests cover read-modify-write preservation and proxy auth cache
  behavior, including malformed password updates.
- Metastore tests cover credential marshal/unmarshal and user result readback.
- HTTP v2 tests cover create, description-only update, and describe response
  propagation.
- CI runs package tests, static check, and Go-only builds against the coordinated
  milvus-proto dependency.

## Rejected Alternatives

- Add a new update-description RPC. Rejected because the existing
  `UpdateCredential` API already owns credential metadata updates and can model
  password and description as independently optional fields.
- Store descriptions in a separate metadata key. Rejected because credentials
  already have a versioned WAL update path and catalog record. A separate key
  would add another consistency edge without reducing update complexity.
- Update proxy auth caches on every credential metadata edit. Rejected because a
  description-only update carries no SHA256 password and would blank cached
  authentication state.
