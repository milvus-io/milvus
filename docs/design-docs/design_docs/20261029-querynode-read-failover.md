# Bound QueryNode read connection attempts

Status: implementation and local verification complete; live eviction test pending.
Base: origin/master 081a8c9bff (rebased 2026-10-30).

## Problem and evidence

During the 2026-10-29 incident, worker 1906 exited while its session remained
registered. Delegator 1914 retried the worker connection until almost the entire
4-second user deadline elapsed. Proxy logged `retried=0, attempt=12`: other
replicas existed, but the first replica returned too late for failover.

QueryNode clients on master, 3.0 and 2.6 use ClientBase. It dials before entering
retry.Handle and again inside a failed iteration. It also redials synchronously
when checkGrpcErr says not to retry. GetGrpcClient holds its mutex during dial,
so concurrent requests can wait beyond their own connection timeout.

## First patch

1. Acquire a client once inside each retry iteration. A failed final iteration
   does not initiate another connection. Reset invalidates the connection;
   only the next attempt reconnects.
2. Share an in-progress connection attempt. Do network I/O outside the state
   mutex. Waiters select on the shared result or their own context. They share
   a failed result instead of serially starting another dial. The initiating
   caller owns the dial context; its cancellation can fail the shared attempt,
   and a subsequent call may start a fresh attempt.
3. Close prevents an in-flight dial from publishing a connection afterward.
4. SearchSegments, QuerySegments and QueryStreamSegments use one worker attempt.
   A failed connection on this one-attempt path returns immediately, without
   consulting the possibly stale session. Proxy owns replica failover. Normal
   query execution retains the parent request deadline; only connection setup
   uses the existing dial timeout. Single-replica reads can now surface a
   transient transport failure earlier; they retain Proxy's bounded retry path.

No protobuf or merr wire projection changes are included. ErrConnect remains
an internal transport sentinel and flows through the existing Status boundary.
Consequently existing master channel-blacklist behavior remains in force.

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

- A failed dial with one attempt performs one address resolution/dial even when
  the node session still exists; three attempts perform at most three dials.
- Closing/mismatched connections return without resolving/dialing again.
- Concurrent waiters can cancel while a dial is blocked; waiters share failure.
- Close during dial cannot resurrect the client.
- Trace transport failure through QueryNode Status and Proxy LB to a different
  leader, under a 4-second request deadline. Verify the final response succeeds.
- Check parent cancellation and normal successful RPCs; run existing client
  retry, delegator and shard-client tests with dynamic,test and disabled inlining.
- A live multi-replica eviction experiment is required before claiming a
  production failover-latency bound; mock tests alone do not establish one.

### Local results on the rebased branch

- `internal/util/grpcclient` full suite passed with `-race`,
  `-tags dynamic,test`, and `-gcflags='all=-N -l'`. It covers one dial per
  attempt, a shared failed dial, independently canceled waiters, Close racing
  with a dial, connection-closing and NodeID-mismatch fast exits, and a
  one-attempt retriable Status without backoff.
- Delegator `TestSearch`, `TestQuery`, and `TestQueryStream` passed with the
  required tags and flags. Each asserts that the actual worker call receives
  a one-attempt context.
- The Proxy `TestDeadWorkerConnectionFailsOverBeforeDeadline` passed. It uses
  a real failed TCP dial through ClientBase, the existing Status round trip,
  and the real LB policy: leader 1 is excluded and leader 2 succeeds while a
  4-second parent context remains live. The full `LBPolicySuite` also passed.
- Source trace: `remoteWorker.SearchSegments` and `QuerySegments` preserve
  the client error; `executeSubTasks` preserves a single failure;
  `QueryNode.Search` projects it with `merr.Status`; Proxy `searchShard` and
  `queryShard` recover it with `merr.Error` and preserve the error through
  wrapping; LB classifies it as a system failure and excludes that leader.
  `ErrConnect` remains generic error code 65535 and non-retriable on the wire.

These checks verify the failed-dial route. They do not measure a live
eviction, a worker RPC that stalls after connection, or a stream that stalls
after the initial QueryStreamSegments call.

The existing `internal/distributed/querynode/client` package suite could not
run in this environment: `Test_NewClient` creates a session against a local
etcd server, which is absent. Setting `ETCD_USE_EMBED=true` alone is
insufficient because that test does not initialize the embedded server.
