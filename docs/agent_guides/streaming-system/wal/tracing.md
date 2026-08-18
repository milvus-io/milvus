# WAL Tracing

> **How to use this guide**: This page defines the semantic shape of WAL
> traces. Use it when changing WAL trace spans, message-carried trace context,
> replication tracing, broadcast tracing, or consume-side trace restoration.
> Read the streaming system and message semantic guides first when the change
> also modifies WAL behavior.

WAL tracing describes the causal path of a logical WAL message through append,
durable persistence, consume, replication, and callback handling.

It is not a per-function profiler. WAL spans are semantic markers for lifecycle
boundaries. If a span does not represent a WAL ownership, persistence, consume,
replication, or callback boundary, it usually does not belong in the WAL trace.

## Global Model

WAL trace context is message-carried. A WAL message stores the trace context
that downstream asynchronous work should use as its parent. When a message
crosses a semantic ownership boundary, the message-carried trace context may be
overwritten to the new parent.

WAL tracing follows these principles:

1. A client request may produce one or more WAL messages.
2. A WAL message carries trace context across asynchronous boundaries.
3. Write-side spans describe how a message enters WAL.
4. Append spans describe concrete persistence attempts for concrete messages.
5. Consume-side spans describe where WAL visibility resumes from stored
   message state.
6. Replication spans describe secondary-cluster ownership of a primary message.
7. Broadcast callback spans describe follow-up work after broadcast delivery is
   acknowledged.
8. TimeTick is intentionally not traced.

The helper APIs live in the message package. This guide defines their intended
trace semantics, not their implementation.

## Span Semantics

| Span | Meaning | Parent | Duration |
|---|---|---|---|
| `wal.autocommit` | Logical WAL write for one non-transactional, non-broadcast message. | Caller request or upstream WAL trace. | Covers the client-side logical append operation. |
| `wal.txn` | Logical WAL write for one transaction, including BeginTxn, body messages, and CommitTxn. | Caller request. | Covers the whole transaction append sequence. |
| `wal.broadcast` | Logical WAL broadcast for one broadcast task across target pchannels or vchannels. | Caller request. | Covers broadcast fan-out and append scheduling. |
| `wal.append` | WAL adaptor append boundary for one concrete message append. | A logical write span such as `wal.autocommit`, `wal.txn`, `wal.broadcast`, `replicate.secondary`, or `wal.dist_append`. | Covers adaptor-level append work. |
| `wal.appendimpl` | WAL implementation append boundary where the concrete backend persists the message. | `wal.append`. | Covers backend append and persistence. |
| `wal.dist_append` | Distributed append marker when a producer writes through a remote WAL. | The logical write span, usually `wal.autocommit` or `wal.txn`. | Covers the remote append request until append completion. |
| `wal.catchup_consume` | Durable backend consume marker for a message read from the local backend scanner during catchup. | Message-carried trace. | Short marker span. |
| `wal.dist_consume` | Distributed or non-local consume marker for a message read through a remote scanner or remote WAL path. | Message-carried trace. | Short marker span. |
| `replicate.secondary` | Secondary cluster receive and re-append boundary for a replicated primary WAL message. | Primary-side consumed message trace, usually under `wal.dist_consume`. | Covers secondary-side replicate handling until append. |
| `wal.bc_callback` | Broadcast ACK callback processing after broadcast message persistence and acknowledgement. | Broadcast message trace. | Covers callback handling such as task completion and cache invalidation. |

`wal.autocommit`, `wal.txn`, and `wal.broadcast` are logical write roots. They
represent user-visible or system-visible WAL write intent.

`wal.append` and `wal.appendimpl` are physical append boundaries. They should
not become logical roots unless the upstream context is missing.

`wal.catchup_consume` and `wal.dist_consume` are resume markers. They are
usually short and exist to reconnect downstream asynchronous work to the
message trace that was stored in WAL.

`wal.catchup_consume` is emitted only when the scanner reads from the durable
backend scanner in catchup mode. It is intentionally absent in tailing mode:
tailing readers consume the same immutable message instance from the
WriteAheadBuffer, and that shared message's properties must not be mutated to
overwrite `_tc`.

`replicate.secondary` is the only replication ownership span. There is no
`replicate.primary` span.

## Span Attributes

Span attributes should explain the message or broadcast being traced without
encoding that information into span names. Keep span names stable and put
message scope, timing, transaction, and broadcast metadata in attributes.

Common message attributes:

| Attribute | Applies To | Meaning |
|---|---|---|
| `message.type` | Message-related WAL spans. | WAL message type. |
| `message.vchannel` | VChannel-scoped messages. | Target VChannel. Empty means the message is PChannel-level or not VChannel-scoped. |
| `message.timetick` | Message-related WAL spans after TimeTick is assigned. | WAL TimeTick of the traced message. This does not make TimeTick messages traceable. |
| `message.replicate` | Message-related WAL spans. | Whether the message carries replication metadata. |
| `txn.id` | Transaction messages and synthetic transaction traces. | Transaction ID. |

Broadcast-specific attributes on `wal.broadcast`:

| Attribute | Meaning |
|---|---|
| `broadcast.id` | BroadcastID of the broadcast task. |
| `broadcast.vchannels` | Target broadcast VChannels. |
| `message.type` | Broadcast message type. |

`wal.broadcast` should make the broadcast target scope visible through
`broadcast.vchannels`, not by changing the span name.

## Canonical Trace Shapes

### Autocommit

Autocommit is the normal path for one non-transactional, non-broadcast message.
A complete trace may include remote append, primary persistence, consume, and
secondary replication:

```text
request span
  wal.autocommit                          # autocommit-specific
    wal.dist_append                       # remote append only
      wal.append
        wal.appendimpl
          wal.catchup_consume / wal.dist_consume  # consume marker, catchup/remote only
            replicate.secondary           # replication only
              wal.autocommit              # secondary re-append
                wal.append
                  wal.appendimpl
```

If the producer already owns a local WAL, `wal.dist_append` is absent and
`wal.append` is directly under `wal.autocommit`. If the message is consumed from
a local durable backend scanner during catchup, the consume marker is
`wal.catchup_consume`; if it is consumed through a remote or distributed
scanner path, the marker is `wal.dist_consume`. A steady-state local tailing
consumer emits no consume marker.

The secondary-side `wal.autocommit` is the local append of a replicated concrete
message into the secondary WAL. It does not mean the original client request was
issued on the secondary cluster.

### Transaction

A transaction has one transaction-level logical write span and several concrete
message appends. A complete trace uses CommitTxn as the point where the
transaction becomes consumable:

```text
request span
  wal.txn                                 # txn-specific
    wal.dist_append                       # remote append only: BeginTxn
      wal.append                          # BeginTxn
        wal.appendimpl
    wal.dist_append                       # remote append only: txn body
      wal.append                          # txn body message
        wal.appendimpl
    wal.dist_append                       # remote append only: txn body
      wal.append                          # txn body message
        wal.appendimpl
    wal.dist_append                       # remote append only: CommitTxn
      wal.append                          # CommitTxn
        wal.appendimpl
          wal.dist_consume                # consume marker for assembled txn
            replicate.secondary           # replication only: BeginTxn
              wal.autocommit              # secondary re-append
                wal.append
                  wal.appendimpl
            replicate.secondary           # replication only: txn body
              wal.autocommit              # secondary re-append
                wal.append
                  wal.appendimpl
            replicate.secondary           # replication only: CommitTxn
              wal.autocommit              # secondary re-append
                wal.append
                  wal.appendimpl
```

If the producer writes to a local WAL, `wal.dist_append` is absent and each
`wal.append` is directly under `wal.txn`.

`wal.txn` is the semantic parent for the whole transaction. BeginTxn, body
messages, and CommitTxn are each concrete WAL messages, so each append has its
own `wal.append` and `wal.appendimpl`.

Downstream consumption uses the transaction assembled at CommitTxn. The
synthetic transaction message is the downstream semantic unit. When that
transaction is expanded later, BeginTxn, body messages, and CommitTxn should use
the transaction-level trace rather than preserving unrelated body-level traces.
This means the expanded child messages may have their `_tc` overwritten from
the assembled transaction's CommitTxn trace; repeated copying of that
transaction trace is intentional.

Txn tracing should stay flat at the logical level. Do not add a separate
`client.append` span or independent per-body logical roots.

### Broadcast

Broadcast has one broadcast-level logical root and multiple concrete appends. A
complete trace may include primary broadcast append, primary callback,
distributed consume, secondary re-append, and secondary callback:

```text
request span
  wal.broadcast                           # broadcast-specific
    wal.append
      wal.appendimpl
        wal.dist_consume                  # consume marker
          replicate.secondary             # replication only
            wal.append
              wal.appendimpl
                wal.bc_callback           # broadcast-specific callback
    wal.append
      wal.appendimpl
    wal.bc_callback                       # broadcast-specific callback
```

`wal.broadcast` represents the broadcast task. Each `wal.append` represents one
concrete append produced by broadcast fan-out.

Broadcast must not create `wal.autocommit` or `wal.txn`. Broadcast is already
the logical write root.

`wal.bc_callback` represents ACK-driven callback work after broadcast delivery,
such as task completion and cache invalidation. It is not a new user request.

## Non-Traceable Messages

TimeTick is intentionally not traced.

TimeTick is a WAL progress and control signal, not a user-visible mutation.
Tracing every TimeTick would dominate trace volume and hide useful message
causality.

Trace propagation tests must not use TimeTick unless the expected behavior is a
trace no-op.

## Invariants

- Span names are stable and low-cardinality.
- WAL tracing is message-causal, not goroutine-causal.
- Message trace context represents the parent for downstream asynchronous work.
- Logical write spans are `wal.autocommit`, `wal.txn`, and `wal.broadcast`.
- Physical append spans are `wal.append` and `wal.appendimpl`.
- Consume spans are short resume markers.
- Replication has only `replicate.secondary`.
- Broadcast does not create `wal.autocommit` or `wal.txn`.
- `wal.broadcast` carries BroadcastID, broadcast VChannels, and message type as
  attributes.
- Transaction downstream fan-out uses transaction-level trace.
- Message-related WAL spans carry message type, TimeTick when available,
  VChannel when applicable, and replication state.
- Transaction messages carry txn ID when available.
- TimeTick is not traced.
