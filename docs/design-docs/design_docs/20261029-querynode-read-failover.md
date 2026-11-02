# Bound QueryNode read connection attempts

## Problem and evidence

During the 2026-10-29 incident, worker 1906 exited while its session remained
registered. Delegator 1914 retried the worker connection until almost the entire
4-second user deadline elapsed. Proxy logged `retried=0, attempt=12`: other
replicas existed, but the first replica returned too late for failover.

QueryNode clients on master, 3.0 and 2.6 use ClientBase. It dials before entering
retry.Handle and again inside a failed iteration. It also redials synchronously
when checkGrpcErr says not to retry. GetGrpcClient holds its mutex during dial,
so concurrent requests can wait beyond their own connection timeout.

## Design

1. Acquire a client once inside each retry iteration. A failed final iteration
   does not initiate another connection. Reset invalidates the connection;
   only the next attempt reconnects.
2. Share an in-progress connection attempt. Do network I/O outside the state
   mutex. Every caller, including the initiator, selects on the shared result,
   its own context, or client closure. They share a failed result instead of
   serially starting another dial. The dial runs in a goroutine under a context
   detached from the initiator's cancellation, while retaining its context
   values for tracing. `DialTimeout` still bounds the gRPC handshake. Canceling
   the first request releases that request promptly without failing other
   waiters or forcing a second dial. This shared connection setup is separate
   from the request's QueryNode RPC, which continues to use the request context.
3. Close cancels the shared dial, releases all callers, and prevents an
   in-flight dial from publishing a connection afterward. The address lookup
   callback has no context parameter, so an address lookup that itself blocks
   cannot be interrupted; Close still releases its callers immediately.
4. SearchSegments, QuerySegments and QueryStreamSegments allow two attempts.
   A failed connection acquisition returns immediately on either attempt,
   without a session lookup or another dial. An RPC error on an established
   connection can retry once; the second failure returns without an additional
   backoff. For QueryStreamSegments, ClientBase can retry only stream creation;
   it never replays a stream after it returns or after data is emitted. Proxy
   owns replica failover. The parent request deadline still bounds RPC
   execution, and the existing dial timeout bounds the gRPC handshake.

The scoped read path no longer derives `ErrNodeNotFound` from a synchronous
session lookup after either a failed dial or a retryable RPC error. Its previous
`markSegmentOffline` side effect therefore does not run for those failures.
A session lookup is rate limited but its duration is not bounded separately;
doing it synchronously after a failed dial
could spend the request's remaining failover budget on etcd. Server-reported
`ErrNodeNotFound` and ServerID mismatch still mark segments offline. QueryCoord
also removes unavailable workers through its own session and distribution
recovery. A later change could restore this signal using a separately bounded
or cached session check.

No protobuf or merr wire projection changes are included. ErrConnect remains
an internal transport sentinel and flows through the existing Status boundary.
Consequently existing master channel-blacklist behavior remains in force.
One transient worker RPC error is absorbed by the local retry and does not
blacklist an otherwise healthy delegator. A second failure or a failed dial
still reaches Proxy, which may blacklist that delegator for the channel.

The cancellation design in
[List Running Requests and Cancel a Request by ID](20260916-running-request-list-and-cancel.md)
requires each canceled request and its QueryNode sub-RPCs to stop together.
This remains true: callers stop waiting as soon as their own context is canceled,
and the eventual RPC still uses that context. Only the shared connection setup
can continue for other callers. The draft also uses `context.WithoutCancel` for
shared QueryNode work when one member cancels. No reviewed design document found
for this client requires an initiator's cancellation to abort other callers'
connection setup.

## Further design

Transport recovery and request retry need separate policies. A future resolver
migration must not copy streaming's unbounded wait-for-ready behavior for a
pinned, unready worker: the existing picker waits while its session remains.
Coordinator RPCs and pinned QueryNode reads require different waiting policies.

Cross-request routing suppression should eventually depend on evidence of a
failed route, independently of Status.Retriable. Mapping ErrConnect to a typed
retriable system error alone would disable today's blacklist insertion. Add
bounded cooldown/probes together with that classification change, and audit
the full wire contract and SDK consumers. Do not mark segments permanently
offline on one transient connection failure or route to another worker without
confirmed segment placement. Replica sweep/backoff scheduling is a separate
Proxy change; this patch retains existing retry intervals.

## Verification

- A failed dial on the scoped worker read path performs one address
  resolution/dial even when the node session still exists.
- One gRPC `Unavailable` on an established unary connection gets one retry and
  succeeds without Proxy blacklisting the delegator. If the retry requires a
  new dial and that dial fails, return immediately for replica failover.
- QueryStreamSegments retries only errors while creating the stream, never after
  output has been emitted.
- Closing/mismatched connections return without resolving/dialing again.
- Concurrent waiters can cancel while a dial is blocked; waiters share failure.
- Cancel the initiating request during an in-flight dial, then make the worker
  available: the initiator returns `context.Canceled` promptly and a live
  waiter receives the same dial's successful connection.
- Close cancels a blocked gRPC handshake promptly, rather than merely releasing
  callers while the dial continues until `DialTimeout`.
- Close during dial cannot resurrect the client.
- Trace transport failure through QueryNode Status and Proxy LB to a different
  leader, under a 4-second request deadline. Verify the final response succeeds.
- Check parent cancellation and normal successful RPCs; run existing client
  retry, delegator and shard-client tests with dynamic,test and disabled inlining.
- A live multi-replica eviction experiment is required before claiming a
  production failover-latency bound; mock tests alone do not establish one.
