# Idempotency fault integration test design

## Purpose

Verify idempotent inserts across ambiguous WAL acknowledgements, client deadlines,
and StreamingNode owner replacement. The suite uses the subprocess MiniCluster
with Pulsar, etcd, and MinIO, and covers both single-message inserts and
multi-message transactions with auto-generated primary keys.

## Fault boundaries

| Scenario | Boundary | Expected behavior |
| --- | --- | --- |
| Lost backend acknowledgement | A successful `walimpls.Append`, before the adaptor evaluates its result | The real retry loop persists another record with the same TimeTick; reader deduplication prevents extra rows |
| Crash before window completion | A successful append, before idempotency `Window.Complete` | SIGKILL followed by owner recovery preserves the original IDs and row offsets |
| Partial shard completion | One shard paused before append or transaction commit; three other shards durably appended | Recovery preserves the committed shards and completes the remaining shard once |
| Client deadline | A successful append paused before window completion | The client times out; releasing the barrier and retrying the same key returns the original IDs |
| Unrelated request | A backend fault armed for a different idempotency key | The unrelated write succeeds without consuming the selected request's one-shot fault |

The backend fault changes the return value at the WAL implementation boundary,
inside the existing adaptor retry loop. Timestamp allocation and interceptor
callbacks remain intact. Changing a successful whole interceptor chain into an
error above this loop would cause an outer retry with a new TimeTick and model a
different failure.

The idempotency boundaries only observe or pause execution. Process termination
and client deadlines supply the actual failures; the controller never forces
`Window.Fail`, `Window.Complete`, or a replication checkpoint result.

## Process isolation

MiniCluster components run in separate processes, so a failpoint enabled in the
test process cannot control a StreamingNode. Standard PingCAP markers are
rewritten in a private source copy to produce an instrumented server fixture,
using the tool version selected by `go.mod` and the normal Milvus build flags.
The ordinary build retains inert markers.

The controller supplies failpoint configuration only to StreamingNode processes
and clears inherited HTTP failpoint controls. Each configuration selects a
process PID, idempotency key, message type, and optionally a vchannel. Matching
happens before an atomic one-shot claim. A replacement process has a different
PID and cannot rearm the previous incarnation's fault.

## Hit receipts and barriers

The server write path atomically publishes a receipt containing the process PID,
key, vchannel, message type, durable message ID, TimeTick, autoIDs, and original
row offsets. A pre-append receipt has no durable message ID. Enabling a failpoint
alone does not prove that the selected write reached its boundary.

Pause barriers have a two-minute bound and a release signal controlled by the
test. Cleanup disarms the configuration and releases barriers. Control files
remain available until all cluster processes stop, preventing a paused process
from missing a release because its directory was removed too early.

## Correctness checks

Backend retries must preserve the logical message's TimeTick while producing
distinct persisted message IDs. This exercises physical reader deduplication,
which exists independently of request-level idempotency.

Request-level checks compare the IDs returned by explicit same-key retries with
the server's original IDs at their recorded row offsets, including after owner
recovery. Queries verify every ordinal/primary-key pair. Flushed collection
statistics verify the exact stored row count because query-level primary-key
deduplication can hide duplicate physical rows. Compaction is disabled to preserve
this check.

## Scope

The suite covers local WAL behavior with Pulsar. Woodpecker, CDC switchover, and
real storage/network chaos require separate coverage. Recovery follows the
upstream WAL summary replay path. Reader duplicate-drop logs and metrics provide
additional diagnostics; metric counters are not currently asserted by the suite.
