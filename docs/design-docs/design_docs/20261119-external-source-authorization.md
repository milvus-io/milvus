# MEP: Retain source authorization for asynchronous external reads

- **Created:** 2026-11-19
- **Author(s):** @jja725
- **Status:** Draft
- **Component:** Proxy, Coordinator, DataNode, QueryNode, Storage
- **Related Issues:** [#55755](https://github.com/milvus-io/milvus/issues/55755)
- **Related work:** [Azure SAS renewal](https://github.com/milvus-io/milvus-storage/pull/817)
- **Released:** Not released

## Summary

Authorize external collection registration and explicit refresh as their
initiating user, and retain a non-secret authorization binding for subsequent
source reads. A deployment-provided credential provider checks that owner's
current source permissions whenever it issues storage credentials.

This proposal integrates the storage library's `SourceReadContext` and retained
credential provider with Milvus's asynchronous work. SAS renewal alone does not
establish whose source permissions authorize a read.

## Motivation

External exploration, indexing, refresh and lazy queries can read source files
after registration returns. Copying the request's user token into a persisted
external specification exposes a bearer credential and cannot support work that
outlives that token. A shared service credential, on the other hand, does not
check the registering user's table access.

The engine needs durable identity metadata that the source authority can
re-authorize, plus a reader-specific callback that obtains short-lived storage
credentials. Native and format caches must preserve that reader's scope.

## Scope

Registration and explicit refresh check their current initiating user. A refresh
that omits the source still requires new authorization. Background reads use the
resulting binding owner. This proposal adds no source permission check for the
current search caller and no second source permission database inside Milvus.

The first credential type is Azure SAS for Lance and Iceberg. Other cloud
providers, detecting a dropped and recreated same-name source table, and importing
bindings through replication/restore are follow-up work.

## Public interfaces

`sourceauth.Install` installs a provider registry and request hook in each
process. The request hook authenticates transient source credentials, strips them
from transport metadata, and attaches `sourceauth.Authorizer` to the request
context. Authorization runs after Proxy resolves the effective source.

The authorizer returns a provider name and non-secret reference. It must not
return a user JWT, SAS, or reference that functions as a bearer capability.
`sourceauth.Registry` opens independent sessions from trusted bindings. A session
exposes its allowed roots, account, credential callback and close operation.

These extension points do not define a vendor-specific identity exchange or
Catalog protocol. A deployment supplies its own authenticated implementation.
Stock binaries install no provider and preserve unbound collection behavior.

## Design details

### Admission and durable metadata

Proxy reserves `credential_binding` as server-owned external-spec metadata.
Public create, refresh and snapshot inputs cannot supply it, including alternate
JSON spellings. Public describe and job responses omit it.

A binding contains a provider reference, authorization-attempt identifier, exact
source and canonical specification digest. The identifier is not a source-table
incarnation. The digest checks substitution; it is not proof of permission or
provenance. Only trusted coordinator metadata may supply a binding to a provider.

An omitted refresh authorizes the resolved stored source. Proxy also sends an
internal fingerprint of the source/spec used for that decision. DataCoord checks
the fingerprint under its collection DDL lock before admitting the job. A
concurrent change returns a retriable stale-metadata error. An explicit replacement
cannot remove an existing binding without going through fresh authorization.

### Native readers and lifetime

Each read session creates a native `SourceReadContext`, passed separately from
storage properties. `Reader::create_with_context` and context-aware exploration,
file-info and filesystem entry points retain the provider. The context follows
index file managers, schema sampling and lazy QueryNode readers.

Go callbacks use `cgo.Handle`, not retained Go pointers. Callback results use
transient C-owned buffers copied by the native ABI before release. Reader-owned
sessions outlive the original Proxy request; job-owned contexts honor job
cancellation. Each credential fetch has a finite deadline. The session closes
after retained native owners and callbacks drain.

The source context validates original URIs against parsed account, endpoint,
container and root boundaries. Routing properties do not authorize access.
Bound sources reject alternate credentials and endpoint overrides, and cannot
use legacy readers that do not accept a source context. Credential acquisition
failure must never fall back to ambient service credentials.

Independent contexts do not share credential, filesystem or dataset cache
authority. The storage implementation must cover metadata, data and delete files,
directory listing, retries and retained streams for each supported format.

### Renewal and failures

The provider rechecks the binding owner's current source permissions before
issuing credentials. Native readers renew SAS through that provider. A revoked
grant prevents subsequent issuance; a previously signed SAS can remain usable
until its signed expiry. This does not renew the user's login token.

The bridge preserves denied, expired, transient, cancelled and invalid-response
outcomes through native status, Arrow C streams and worker RPCs. Only transient
source failures request retry. Diagnostics omit credentials and provider details.

## Compatibility, deprecation and migration

This is an integration draft. The storage source-context API is not in the
currently pinned upstream dependency. A reviewed native dependency pin, complete
Linux build and failure-path verification are required before merging or enabling
the implementation. Merely merging this engine patch cannot enable user-bound reads.

Unbound collections retain legacy behavior. Deployments must review or re-register
them if they require source-user authorization everywhere. All processes must
install consistent providers. A bound collection on an unconfigured worker fails
closed.

Replication, backup and restore must not turn client-controlled metadata into
trusted bindings. An enabling distribution must refuse these ingress paths or
re-admit imported sources before release. Public snapshot input rejection is
included; this draft does not claim a complete replication ingress audit.

Older binaries cannot safely process bound collections. Rollback requires draining
their source jobs/readers and disabling those collections or retaining a binary
that supports the source-context contract.

## Test plan and current verification

The sourceauth, external-spec and error packages pass race-enabled Go tests on
master with its required Go toolchain. These cover injection rejection, refresh
by another caller, binding removal/substitution, concurrent refresh, missing
providers, session isolation and error propagation.

Included native tests call actual exploration, file-info, filesystem and packed
reader entry points with failing providers. They exercise five typed outcomes,
scope rejection, owner release, Azure routing and Arrow C stream/CStatus behavior.
They have not yet run against a complete Linux engine build.

Before merge, run native tests, the full `make test-go` error-contract check and
Proxy/DataCoord checks against the reviewed storage revision. Then validate real
Lance/Iceberg registration, refresh, indexing and query reads with allowed and
denied users, renewal, revocation, cancellation, restart and concurrent owners.
Inspect persisted schemas/tasks and logs for accidental credential retention.

## Rejected alternatives

- A global service credential cannot represent the initiating user's source ACL.
- Persisting a user token ties background work to token expiry and exposes bearer
  credentials in metadata, jobs and backups.
- Checking only the current search caller leaves asynchronous registration,
  indexing and refresh reads without an initiating-user authorization binding.

## References

- [Milvus storage](https://github.com/milvus-io/milvus-storage)
- [Azure SAS renewal for retained Iceberg readers](https://github.com/milvus-io/milvus-storage/pull/817)
