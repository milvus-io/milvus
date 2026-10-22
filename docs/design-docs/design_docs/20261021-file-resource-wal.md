# File Resource Metadata in WAL

- **Feature DRI:** TBD
- **Primary Approver:** TBD
- **Independent Approver:** TBD
- **Design Review:** TBD

## Scope

`AddFileResource` and `RemoveFileResource` currently change RootCoord's catalog
directly. Those operations never enter WAL, so CDC cannot replay resource
registrations or removals. Move their metadata writes into broadcast ACK
callbacks, following the existing coordinator DDL path.

The public requests and responses stay unchanged. This change makes resource
metadata mutations eligible for CDC; it does not claim complete replication of
collections that depend on external files. Object transfer, existing-resource
bootstrap, and cross-PChannel resource readiness are separate work.

## Messages and ownership

| Message | Version | Header | Body | Broadcast resource key |
| --- | --- | --- | --- | --- |
| AddFileResource | V2 | Empty | FileResourceInfo: ID, name, path | ExclusiveCluster |
| RemoveFileResource | V2 | Empty | Resource ID and name | ExclusiveCluster |

Both messages target CChannel only and carry no `Unreplicable` property.
Resource metadata is business content and belongs in the body. No new public
API or configuration is needed. Message builders and reflection are generated
from `pkg/streaming/util/message/codegen/reflect_info.json`.

RootCoord remains the owner of the resource catalog. A new registration's ID is
allocated on the request path and recorded in the message. ACK callback retries
and secondary replay use that ID without allocating another one. Each cluster
maintains its own resource-list sync version; that version is not an artifact
content version and is not copied from the source cluster.

## Request and replay paths

Add validates the name and object existence, acquires ExclusiveCluster, then
checks existing metadata. An identical name and path reuse the existing ID and
skip the size admission check. A different path under the same name is rejected
before broadcasting. When a new resource needs size admission, the request
releases the guard for object-storage I/O, then reacquires it and rechecks current
metadata. A concurrent identical registration reuses its ID; otherwise the new
resource obeys the current `common.fileResource.maxFileSize` and receives an ID.
The request broadcasts the resolved resource record while holding the guard.

Remove acquires ExclusiveCluster and checks whether the named resource is
referenced. A resource in use is a request error returned before creating a
broadcast task. Removing an absent name succeeds without creating a task.
Existing legacy registrations with an empty name can still be removed. The
message records both the name and the resolved ID.

The Add callback writes the exact resource record. Replaying an identical ID,
name and path is a no-op and does not advance the sync version. A conflicting
ID or name is a system inconsistency, not a successful idempotent add.

The Remove callback removes only the recorded ID under the recorded name.
A missing record, or a newer registration under the same name, is a no-op.
If a reference still exists during replay, the callback returns a system
unavailable error so the broadcast remains pending. Catalog failures leave
in-memory metadata and the sync version unchanged.

After a successful apply, both callbacks notify the FileResourceObserver,
including on an idempotent retry. The observer syncs metadata and downloads to
the configured QueryNode, DataNode and Proxy targets through its existing retry
loop. Downloads are not part of ACK callback completion: an unavailable node
must not retain the global broadcast guard indefinitely.

The source API waits for broadcast completion, then preserves its existing
synchronous Observer.Sync attempt. A sync failure returns the existing
"metadata operation succeeded but some node sync failed" error and schedules
another observer attempt. Metadata is not rolled back. Remove of an absent
resource still succeeds without a synchronous sync attempt.

## Ordering and recovery

ExclusiveCluster serializes resource mutations with all schema broadcasts,
which hold SharedCluster. On the request path this prevents a schema change
from acquiring a new reference between Remove validation and metadata apply.
It also serializes same-name registrations without adding another resource
domain or changing every schema caller.

Once a broadcast task exists, caller cancellation does not undo the accepted
operation. The broadcaster retains ownership and retries through catalog
failure or coordinator restart. Callbacks are registered with RootCoord's DDL
callbacks. Repeated apply must not allocate IDs, double-increment versions, or
delete a later same-name registration. No catalog key layout changes.

CDC forwards the new messages through the generic WAL replication path. The
secondary reconstructs the broadcast task and invokes the same callbacks;
request-side storage admission is not rerun there. ExclusiveCluster orders
these metadata callbacks against schema callbacks on CChannel.

This does not establish a barrier between a CChannel callback and schema/DML
processing on another PChannel. Secondary resource readiness before analyzer
or function-runner construction, and collection reference accounting on replay
and promotion, need a separate end-to-end audit before advertising full CDC
support for resource-backed collections.

## Compatibility and operations

Both clusters must support these message types before they are produced.
There is no downgrade conversion for a WAL containing the new messages.
Existing registrations remain readable and retain their IDs. Re-registering
one emits an Add message with the original ID; enabling CDC does not itself
backfill old registrations.

The source path is preserved exactly. The secondary must be able to access the
same object through its configured storage, or operators must provision a
matching object. This change adds no storage credential forwarding, copying,
path rewriting, file-content versioning, or content checksum protocol.

Existing request metrics and generic WAL tracing cover the operations; no
metric labels or error-code projections change. Persistent catalog errors or
conflicting replay metadata keep the broadcast pending and require repair.
Node download failures are reported by the existing observer and API behavior.

## Verification

- Existing API tests retain missing-name, missing-object, storage error,
  disabled/enabled size-limit, duplicate-name, allocation-error, abnormal-state
  and successful Add/Remove coverage through the registered callbacks.
- Verify the resource ID and empty control-only fan-out in the outgoing Add,
  and verify no metadata write when a broadcast task is not created.
- Apply each callback repeatedly and assert a single catalog write/version
  increment and repeated observer notifications.
- Inject catalog errors before successful retry and assert maps, versions and
  original error identity remain intact.
- Reject referenced resources before broadcast; keep a callback-time reference
  conflict retriable. Verify stale Remove does not delete a new registration.
- Inject API-side node sync failure after committed metadata and verify Add
  and Remove retain their committed effects and notify the observer.
- Build and run focused Go tests with the required dynamic/test tags and
  disabled optimization. Live dual-cluster file availability, cross-PChannel
  readiness, bootstrap and failover are not validated by these unit tests.

## Alternatives

A file-resource-specific lock would require adding matching locks to every
schema mutation. ExclusiveCluster reuses the existing DDL protocol for these
infrequent metadata operations. Holding that guard until node downloads finish
would let a storage outage block unrelated DDL, so download retries stay with
the existing observer.
