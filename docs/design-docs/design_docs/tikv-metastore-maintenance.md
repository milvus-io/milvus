# TiKV metastore runtime maintenance

This change completes the TiKV metadata operations used by StreamingNode
checkpoint publication and adds opt-in garbage collection for a dedicated TiKV
cluster. It does not include a metadata migration tool.

## Metadata compatibility

TiKV cannot store an empty value. Milvus encodes it with the existing reserved
sentinel and decodes that sentinel on reads, prefix walks, and value-predicate
comparisons. An existing key with an empty logical value is still an existing
key; existence predicates must not treat it as missing. Writing the sentinel as
a user value remains rejected.

StreamingNode creates its first consume checkpoint with
`CompareVersionAndSwap(key, 0, value)`. TiKV implements this case with the existing
transactional `KeyNotExist` guard. Exactly one concurrent creator may succeed;
a rejected guard returns `false` without overwriting the checkpoint. Storage
failures remain errors. Nonzero per-key versions remain unsupported because
TiKV does not expose etcd's per-key version counter. Later checkpoint updates
continue using the catalog's existing value and publisher-term guards.

## Opt-in garbage collection

TiKV retains overwritten and deleted versions until its GC safe point advances.
The active MixCoord starts a worker alongside its other background services when
TiKV is the metadata store. The worker waits for an interval before each round
and advances the safe point only when `tikv.gc.enabled` is true.

| Setting | Default | Meaning |
| --- | --- | --- |
| `tikv.gc.enabled` | `false` | Explicitly opt in to Milvus-managed GC. |
| `tikv.gc.lifeTime` | `600` seconds | Age of metadata versions eligible for GC. |
| `tikv.gc.interval` | `600` seconds | Delay between GC rounds. |

Enable this worker only for a dedicated TiKV cluster managed by Milvus. A safe
point applies to the entire TiKV cluster, not to a Milvus metadata prefix. Do
not enable it on a cluster shared with TiDB or another GC owner. Both TiDB and
this worker use the `gc_worker` service safe point and would overwrite each
other's retention policy. Choose a positive retention period long enough for
metadata transactions and a positive scheduling interval.

Each round obtains the TiKV timestamp, subtracts the configured retention, and
registers the candidate with PD. An older service safe point, such as one held
by a backup, caps the candidate. The client resolves old locks and advances the
GC safe point. The metric `milvus_meta_tikv_gc_safe_point_unix_seconds` records the resulting
safe point as Unix seconds only after a successful round.

A failed timestamp, service-safe-point update, or GC call is logged and retried
on a later round. MixCoord shutdown cancels the worker and waits for the active
round before closing the TiKV client. Configuration refresh affects subsequent
rounds. Disabling the worker cannot undo an already advanced safe point and
does not interrupt a round already in progress.

Retention and interval values are validated on each round, including refreshed
configuration. Nonpositive, malformed or overflowing retention skips the round
before any PD mutation or GC; a retention extending before the timestamp epoch
is also rejected. Fractional retention is rounded up to TiKV's millisecond
precision. An invalid interval uses the ten-minute default, including while GC
is disabled, so it cannot create an immediate retry loop. Timestamp requests
use the worker context and are canceled during shutdown.

The worker remembers only candidates whose lock scan and GC update both
succeeded. It still refreshes its service registration each round, but skips
the scan if the capped candidate has not advanced. Failures remain retryable on
the next round; a backup releasing its pin allows progress again. This local
record is not persisted, so restarting the worker may repeat one scan.

## Verification

Regression coverage includes empty-value predicates, guarded concurrent first
creation, checkpoint updates and stale publishers, service-safe-point pinning,
and enabled/disabled worker scheduling. The local transactional TiKV test
fixture is an in-memory client-go store; it does not establish real TiKV disk
reclamation or production retention timing. Those require a dedicated TiKV
deployment with representative transaction and backup lifetimes.
