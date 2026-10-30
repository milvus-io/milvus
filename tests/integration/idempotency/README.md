# Idempotency fault integration tests

These tests use the existing subprocess MiniCluster and real Pulsar, etcd and
MinIO. They exercise single-message inserts and multi-message transactions with
auto-generated primary keys.

| Scenario | Injection boundary | Assertions |
| --- | --- | --- |
| Lost backend acknowledgement | Successful WAL implementation append, inside the adaptor retry loop | Same TimeTick, different persisted message IDs, stable autoIDs across retry/reopen, exact stored row count |
| Crash before window completion | Successful append, before idempotency `Window.Complete` | SIGKILL of the receipt's process, recovered original IDs and row offsets, exact rows |
| Partial shard completion | Pause one shard before append/commit; observe three persisted shards | Preserve IDs of committed shards, finish the fourth shard once |
| Client deadline | Pause after append, let the client deadline expire, release | Retry returns original IDs and repeated retries do not add rows |

Every scenario requires receipts from the actual server write path. Enabling a
failpoint alone never counts as coverage. Query results check every ordinal/ID
pair; flushed collection statistics also check row count because queries can
hide duplicate physical rows with the same primary key. Compaction is disabled.

## Running

With the native SDK and integration dependencies already prepared:

```sh
make integration-test-failpoint
```

This target builds a private source copy with the `failpoint-ctl` version selected
by `go.mod`, then uses the normal `make build-go` flags. It never rewrites the
checkout or replaces `bin/milvus`. The fixture lives in
`cmake_build/failpoint-fixture` and must be retained until all test processes exit.
`make integration-test` includes this stage after the ordinary suites.

The remote development runner needs no special build flags:

```sh
milvus-dev-cli go-integration local . -b master --allow-dirty \
  -t tests/integration/idempotency
```

Select one leaf case during development with Go's slash-separated run pattern:

```sh
milvus-dev-cli go-integration local . -b master --allow-dirty \
  -t tests/integration/idempotency \
  -k '^TestIdempotencyFailpoints$/^TestBackendAcknowledgementLost$/^single$'
```

Case selection limits execution; the server and selected test package still need
to be compiled.

When no `MILVUS_FAILPOINT_WORK_DIR` is supplied, the shared `TestMain` helper builds
one temporary fixture before running this package and removes it after teardown.
A CI runner that prebuilds can set that variable to reuse the fixture instead.

## Control and cleanup

Standard `failpoint.Inject` markers are inert in the ordinary, unrewritten build.
The instrumented server reads `GO_FAILPOINTS` only on StreamingNode processes;
this suite clears inherited HTTP failpoint controls. A point's value names a
private JSON configuration file. Configuration selects an exact process PID,
idempotency key, message type, and optionally vchannel before claiming a one-shot
hit. Replacements receive a new PID and cannot accidentally rearm the old fault.

Receipts contain the PID, key, channel, message type, durable message ID, TimeTick,
autoIDs and original row offsets. Atomic publication avoids partial reads. Pause
barriers have a two-minute bound and can be released by cleanup; their files stay
alive until the entire cluster stops. No probabilistic injection or timing-only
sleep is used to enter a crash window.

These cases cover local WAL/idempotency behavior. They do not claim CDC switchover
coverage, nor do they turn errors injected above the WAL retry loop into evidence
about backend acknowledgement loss. Recovery correctness follows the current
upstream WAL summary replay, not the historical QA checkpoint/CAS hooks.

## Fault interpretation

The backend fault changes the result immediately after `walimpls.Append`, inside
the adaptor's retry loop. The real retry, timestamp allocation, and interceptor
callbacks remain intact. Converting a successful *whole interceptor chain* into
an error above this loop would instead cause a new append with a new TimeTick;
that injection does not establish a reachable backend failure.

The idempotency boundaries only observe or pause execution. SIGKILL and client
deadlines supply the actual failure; the controller never forces `Window.Fail`,
`Window.Complete`, or a replication checkpoint result.

Same-TimeTick backend retries exercise physical reader deduplication, which also
exists without request idempotency. Request-level coverage comes from explicit
same-key retries, original autoID/offset comparisons, owner recovery, and exact
stored row counts. Reader duplicate-drop logs are useful additional execution
evidence; this suite does not currently assert their metric counters. The current
MiniCluster uses Pulsar, so these results do not establish Woodpecker or CDC
behavior under real storage/network faults.
