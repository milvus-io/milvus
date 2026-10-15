# MEP: Optimistic CAS for Partial Updates

- **Created:** 2026-06-01
- **Feature DRI:** @weiliu1031
- **Primary Approver:** @chyezh
- **Independent Approver:** @liliu-z
- **Design Review:** 2026-08-07
- **Component:** Proxy / StreamingNode / Streaming
- **Related Issues:** [#49980](https://github.com/milvus-io/milvus/issues/49980)
- **Released:** N/A

## Summary

The Strong-read extension at the end of this document replaces the first
fixed-snapshot reads with Strong reads for both initial attempts and CAS
retries. It assumes every participating component is upgraded and adds actual
query snapshot reporting and a WAL lifecycle history floor. The extension is
pending review; the review metadata above applies to the original CAS design.

Milvus partial update currently uses a read-merge-write flow. Proxy reads the
current row, merges the user-provided fields into a complete row, and writes a
standard Delete/Insert transaction. Two concurrent requests can read the same
snapshot and silently overwrite each other's changes because commit does not
validate the snapshot used by the merge.

This proposal keeps query and merge in Proxy and adds optimistic commit
admission at StreamingNode. Every attempt follows this order:

```text
resolve all touched PChannel terms
  -> Strong query and collect actual per-channel readTS
  -> merge
  -> commit(term, readTS)
```

StreamingNode maintains an in-memory, per-WAL-term index of recent primary-key
writes. CommitTxn freezes and drains its body, maps its PKs to a fixed stripe
table for its vchannel, acquires shared stripes for ordinary writes or exclusive
stripes for local CAS, and then allocates its final timestamp. CAS validates the
term, read snapshot, and exact PK versions before persisting CommitTxn. Locks
remain held through version publication, transaction completion, acknowledgement,
and MVCC publication. Different vchannels use separate tables. Disjoint stripes
can proceed concurrently; different PKs that hash to one stripe contend. The
existing lifecycle locks continue to serialize exclusive DDL.

The design does not add a public RPC, SDK field, configuration option,
partial-update-specific WAL message type, or persistent row-version store.
Downstream consumers continue to receive standard Delete/Insert transactions.

## Motivation

Consider a row with two independently updated fields:

```text
initial row:       {pk: 1, name: "old", score: 10}
request A updates: {pk: 1, name: "new"}
request B updates: {pk: 1, score: 20}
```

Without commit validation, both requests can read the initial row. Request A
writes `{name: "new", score: 10}` and request B writes
`{name: "old", score: 20}`. Whichever commits last destroys the other update.

Proxy already owns the complete partial-update semantics, including nullable
and default values, dynamic fields, generated function output, relative array
operations, partition-key validation, and row merge. Moving query and merge to
StreamingNode would duplicate these responsibilities and add QueryCoord and
QueryNode dependencies to the WAL owner.

StreamingNode already owns the WAL ordering point. Commit-side optimistic
validation therefore provides the required lost-update protection while
preserving the existing read and merge path.

### Goals

- Prevent lost updates when concurrent partial updates modify the same PK.
- Keep query routing, schema handling, function generation, and merge in Proxy.
- Keep the persisted data path as standard Delete/Insert WAL transactions.
- Reject stale attempts after a PChannel term change.
- Preserve correctness across WAL recovery and transaction replay without
  rebuilding historical row-level state.
- Bound the memory used for recent PK versions and fail closed when the retained
  history is incomplete.
- Retry only operations that can safely rebuild the complete request.

### Non-Goals

- Cross-vchannel or cross-collection atomicity.
- Strict serializability.
- A persistent row-version store.
- Transaction-level exactly-once semantics or an idempotency token.
- Automatic replay of relative `ARRAY_APPEND` or `ARRAY_REMOVE` operations
  after a deterministic CAS conflict.
- Correctness when partial update runs concurrently with Import,
  RestoreSnapshot, or backfill visibility changes.
- A feature flag or runtime activation gate.

## Public Interfaces

### Public API and SDK behavior

No public RPC or SDK request field is added. Existing partial-update requests
continue to use the current Upsert API.

The observable behavior changes are:

- A conflicting replacement update is rebuilt and retried by Proxy.
- A conflicting relative update returns
  `ErrCollectionPartialUpdateConflict` with `Retriable=false`.
- A replacement update that exhausts its retry budget returns
  `ErrServiceUnavailable` with `Retriable=true`.
- AutoID partial update preserves existing PKs. Missing rows receive new AutoIDs
  and use insert semantics; the mutation result returns the destination PKs in
  request order.

Ordinary non-partial AutoID Upsert follows
[Preserve Primary Keys in Full AutoID Upsert](20260803-autoid-upsert-primary-key-preservation.md):
it preserves an existing PK and inserts with a newly generated AutoID when the
lookup PK is missing. That behavior is independent of
the Partial Upsert CAS protocol described here.

### Internal protocol

The internal message proto adds attempt-scoped commit-admission proof:

```proto
message PartialUpdateCAS {
    uint64 read_ts = 1;
    int64 observed_pchannel_term = 2;
}
```

`read_ts` and `observed_pchannel_term` cannot be derived by StreamingNode
and therefore come from Proxy. Collection and PK identity are not duplicated
in the proof: StreamingNode reads `collection_id` and `schema_version`
from the Insert header, then resolves the authoritative PK descriptor from
ShardManager.

The internal streaming error enum adds:

```proto
STREAMING_CODE_PARTIAL_UPDATE_RETRYABLE = 17;
```

The metadata is encoded in the Insert body's
`MsgBase.Properties["_puc"]`. The outer message property uses the same key
with an empty value as a control marker. The outer marker contains no PK,
`readTS`, term, or other user data.

### Configuration and metrics

The proposal adds one internal, non-refreshable StreamingNode configuration
parameter. The remaining values are internal constants:

| Parameter or constant | Default | Purpose |
|---|---|---|
| `defaultVersionIndexTTL` | 30 seconds | Limits how long each vchannel retains recent PK-write versions and also bounds the valid window from `readTS` to `commitTS` for one partial-update attempt. When the window is exceeded, StreamingNode returns a retryable CAS rejection; Proxy automatically retries only replacement updates that can be rebuilt safely. |
| `streaming.partialUpdate.versionIndexMaxBytes` | 640,000,000 bytes, approximately 610 MiB | Caps the estimated memory used by all PK-version indexes on one StreamingNode. When the shared budget is unavailable, ordinary writes continue, but CAS on the affected vchannel fails closed until the omitted write leaves the valid read window. |
| `partialUpdateCASMaxRetryAttempts` | 5 attempts, including the first attempt | Limits the total number of Proxy attempts used to rebuild a complete replacement update after a deterministic CAS conflict. Requests containing relative field operations do not use this automatic retry. |
| `partialUpdateCASRetryBackoff` | Starts at 10 ms with exponential backoff; each sleep is capped at 40 ms | Controls the delay between Proxy CAS attempts and reduces sustained contention caused by immediately retrying concurrent conflicts. The 40 ms cap is calculated as `4 * partialUpdateCASRetryBackoff`. |

The PK-index budget is an estimate, not a hard process-RSS limit. Each Int64 PK
entry is charged 128 bytes; each VarChar PK entry is charged
`128 + PK byte length`. StreamingNode exports node-level used-byte,
configured-limit, and missed-write metrics for this shared budget.

### Durable format

The proposal adds no new WAL message type. BeginTxn, Delete, Insert, and
CommitTxn keep their existing formats and transaction semantics. CAS metadata
uses an existing properties map inside the Insert body, and the commit marker
uses the existing outer properties map.

No persistent PK index or schema migration is introduced. Pending PK write sets
are reconstructed from existing recovery bodies before accepting writes.

## Design Details

### Correctness invariants

The design depends on seven invariants:

1. **Attempt proof:** a term snapshot and `readTS` belong to the same attempt,
   and terms are resolved before the Strong query starts.
2. **Exact query snapshot:** every CAS `readTS` is the nonzero snapshot
   reported by that channel's successful Strong query.
3. **Atomic admission boundary:** local CAS validation, CommitTxn append, write
   publication, transaction transition, final acknowledgement, and MVCC
   publication retain the same exclusive PK locks. Every ordinary or replicated logical
   write with recoverable PKs participates through shared locks on the same PK
   keys. Schema-less legacy Inserts retain their original acceptance and are
   explicitly exempt from row-lock CAS protection.
4. **Complete write coverage:** every supported WAL write that changes logical
   row data updates either exact PK versions or a conservative fence.
5. **Fail closed:** missing history, malformed proof, or incomplete local CAS
   recovery never degrades to an unchecked ordinary commit.
6. **Authoritative write identity:** a CAS Insert derives its collection and
   schema version from the Insert header and its PK field from ShardManager;
   attempt proof never supplies a competing collection or PK identity.
7. **Timestamp order:** writers acquire their row locks before
   allocating the final visibility timestamp. A CommitTxn freezes and drains
   its body before selecting the complete PK write set. Its provisional
   acker prevents cleanup from overtaking body preparation and is acknowledged
   before waiting for row locks. Only the final timestamp is persisted, after
   a lease recheck that rejects transactions expired during the wait.

### Architecture and state ownership

```mermaid
flowchart LR
    Client[Client] --> Proxy[Proxy read / merge / retry]
    Proxy -->|resolve current term| Assignment[Streaming assignments]
    Proxy -->|query at readTS| QueryNode[QueryNode]
    Proxy -->|Delete + CAS Insert| Producer[Streaming producer]
    Producer -->|BeginTxn / body / CommitTxn| Lock[lock interceptor]
    Lock --> TimeTick[TimeTick interceptor]
    TimeTick --> Guard[PK row locks]
    Guard --> Shard[shard interceptor]
    Shard --> CAS[partial-update interceptor]
    CAS --> WAL[WAL backend]
    CAS --- Index[per-WAL PK state / node-wide byte budget]
```

| State | Owner | Lifetime | Persistence |
|---|---|---|---|
| Original partial payload, attempt `readTS`, and vchannel term snapshot | Proxy `upsertTask` | One client request; rebuilt per retry | None |
| Local transaction packaging and empty `_puc` commit marker | Producer | One vchannel message group | Standard WAL properties |
| PChannel global lock and keyed vchannel RW locks | Lock interceptor | One WAL instance | None |
| Shared ordinary-write and exclusive CAS locks on per-vchannel stripes | StreamingNode process-wide row-lock manager, acquired by TimeTick through `RowLock` | Entries live until their last acquiring or admitted operation releases them | None |
| `pendingTxn`, PK versions, collection fences, and incomplete-txn fences | Partial-update interceptor | One PChannel WAL term | None |
| PK-version byte budget and aggregate used/limit/missed-write metrics | Partial-update interceptor builder | One StreamingNode process | None |
| Live and recovered transaction sessions | TxnManager | Transaction and WAL recovery lifecycle | Existing TxnBuffer recovery |
| Collection schema and immutable PK descriptor | ShardManager | Collection lifecycle in one WAL | Existing recovery snapshot |

Each WAL open creates independent PK, fence, and transaction state, while all
WALs in the StreamingNode process share one row-lock manager and one byte budget.
The row-lock manager maps each active vchannel to a table of 4096 RWMutexes.
The table payload is about 96 KiB, plus bookkeeping and allocator overhead.
Each acquiring operation pins the table before waiting and releases its reference
after unlocking. The last release removes the vchannel entry and returns the
table to a sync.Pool. A holder or waiter therefore prevents a different table
from being installed for the same vchannel, including across WAL lifecycles.
Closing a WAL or dropping a collection does not reset active tables. Proof
state and the exact PK version index remain per WAL; pending write sets are
reconstructed from the existing transaction recovery snapshot.

### End-to-end flow

```mermaid
sequenceDiagram
    participant P as Proxy
    participant A as Assignment / TSO
    participant Q as QueryNode
    participant R as Producer
    participant S as StreamingNode
    participant W as WAL

    P->>A: Resolve every touched PChannel term
    P->>Q: Strong Query with initial GuaranteeTS = MvccTS = 0
    Q-->>P: Complete rows and actual readTS for each channel
    P->>P: Merge fields and build Delete / Insert
    P->>R: AppendMessages with CAS metadata
    R->>S: BeginTxn
    R->>S: Delete / CAS Insert body
    R->>S: CommitTxn with empty _puc marker
    S->>S: Retain shared shard lifecycle lock
    S->>S: Reserve provisional acker; freeze and drain transaction body
    S->>S: Ack provisional timestamp before row-lock admission
    S->>S: Acquire sorted, deduplicated exclusive stripes
    S->>S: Allocate final commitTS; recheck transaction lease
    S->>S: Validate term / window / fences / PKs
    alt deterministic admission reject
        S-->>R: PARTIAL_UPDATE_RETRYABLE
        R-->>P: Return without producer-side replay
        P->>P: Rebuild the full REPLACE attempt
    else commit accepted
        S->>W: Append CommitTxn
        S->>S: Publish PK / fence state; finish transaction
        S->>S: Ack final commit; publish MVCC
        S->>S: Release PK row locks and shard lifecycle lock
        S-->>R: commitTS
        R-->>P: Append response
    end
```

Proxy fans the request out with the existing PK hash or namespace-sharding
rules. Each vchannel group is an independent transaction. The design does not
make a multi-vchannel request atomic.

Transaction-body append records the union of durable Insert and Delete PKs,
including duplicates, without publishing committed versions or acquiring PK
guards. Commit preparation prevents new body admission and waits for all
already admitted bodies before taking a snapshot of this union. A single local
CAS Insert still uses this transaction path. After CommitTxn is durable, normal
consumer transaction assembly delivers the complete Delete/Insert transaction
to downstream consumers; QueryNode visibility continues to follow the existing
commit and TimeTick contract.

On deterministic admission rejection, StreamingNode rejects the transaction
without appending CommitTxn or publishing its writes, then acknowledges any
remaining reserved timestamp and releases its guards. Proxy rebuilds a replacement
attempt with a new Strong query and newly reported per-channel snapshots.
Relative operations and unknown append outcomes retain the no-replay rules
described below.

### Proxy attempt construction

Proxy builds the first attempt and every eligible retry in the same order:

```text
clone original user fields and overlay previously allocated AutoIDs by row
  -> generate function output
  -> resolve and record terms for all candidate write channels
     (all collection shards for AutoID, or the fixed namespace channel)
  -> enqueue a new Strong Query with GuaranteeTimestamp = MvccTimestamp = 0
  -> collect actual nonzero readTS from each candidate write channel's successful response
  -> classify existing and missing rows
  -> normalize fields, validate row alignment and insert fields for missing rows
  -> if an unconverted AutoID PK is missing: allocate
     its destination PK, record the row-to-ID mapping, update the working PK column
  -> select final write channels from this query's captured terms and read timestamps
  -> merge without querying freshly allocated IDs
  -> attach that channel's term and readTS to every CAS Insert chunk
```

The outer task ID and `BeginTs` remain stable across retries. Each Strong query
receives its own BeginTs from normal query scheduling; Proxy does not allocate
a separate CAS read timestamp. Strong optimization may use the current WAL MVCC
barrier, and QueryNode reports the snapshot actually used for the returned rows.
Every retry discards prior proofs and query results before rebuilding. WAL
lifecycle floor validation ensures a snapshot cannot precede the history covered
by the current owner's index, even if the PChannel term did not change.

### Query and merge semantics

`queryPreExecute` shares Strong retrieval, row classification, and Existing-row
Delete preparation with Full AutoID Upsert. Only Partial collects the executed
channel snapshots for CAS. Full requests only PKs and returns before
merge or CAS preparation. Partial still queries and merges before
`insertPreExecute` validates the complete payload, so sharing classification
does not change omitted-field semantics, read consistency, or CAS retries.

Proxy retains the existing merge implementation, including:

- nullable and default values;
- dynamic fields;
- generated function output;
- relative array operations;
- compact nullable-vector representation;
- partition-key immutability validation.

For AutoID collections, a PK present in the query snapshot keeps its identity.
A missing PK selects insert semantics and receives a fresh AutoID. This decision
uses the source row's query snapshot; a later insert of the supplied source PK
does not redirect the newly allocated entity back to that source PK. Required
insert fields must be supplied, while nullable/default fields use the ordinary
partial-update insert merge rules.

If a required field is missing, Proxy returns a non-retriable parameter error
identifying the first missing PK, explaining
that the PK was absent from the query scope and insert fallback needs the named
field. For example: `partial update: primary key 100 does not exist
in the query scope; cannot insert a new entity: missing required field "vector"`.
The query scope includes the requested partition and namespace; absence here
must not be interpreted as absence from every partition in the collection.

Proxy keeps the original user fields immutable and stores allocated AutoIDs in
a separate map keyed by the original request row offset. Initial preparation
and CAS retries share `prepareUpsert`: clone the original fields, overlay
allocated IDs, generate function output, prepare CAS terms, query, and merge.
`checkUpsertPrimaryFieldData` centralizes PK validation, conversion,
replacement, collision checking, and ID parsing. It reuses the existing
primary-field generation and field-update helpers and publishes a replaced
column only after all checks pass. The task owns allocation and retry state;
the PK helper does not access the allocator or WAL. The PK payload type and row count are
validated before allocating IDs. Primary keys are not supported as function
inputs; allocating AutoIDs retains the existing function outputs.
Field normalization and alignment validation follow row classification and
precede allocation. Allocation replaces only the working PK column; it does not restore
the original payload or discard normalized values.
Each attempt executes one Strong query. Freshly allocated AutoIDs take insert
semantics without another existence query. Before reading, Proxy captures terms
for every possible destination channel (all collection shards for AutoID, unless
namespace routing fixes one channel), then binds the actual query snapshots.
After allocation it retains only the final write channels and their original
proofs; missing proofs fail closed. Terms and timestamps are not resampled after
allocation. This relies on the normal AutoID allocator's uniqueness guarantee.
AutoID and non-AutoID collections share row classification, required-field
validation, and merge. Allocation consumes the missing-row offsets, and merge
uses the same classification.
Full AutoID Upsert shares `prepareUpsert`, classification, and the
`allocateMissingAutoIDs` batch allocator, but does not restore Partial retry
state, merge old fields, or prepare CAS proofs. Only Partial saves allocated
IDs and request-order results for retries; its CAS protocol is unchanged.
The original input is never overwritten by allocated IDs or merged values.
After each successful allocation, Proxy saves the complete destination IDs in
request order. Message packing continues to use merge order; `PostExecute`
publishes the saved IDs without looking up or parsing the PK column again.

Allocated IDs remain stable throughout the request's internal CAS retries,
even when still absent. If another vchannel already committed a generated row,
the next read sees that destination row and rebuilds an update at the same PK.
This retains the existing REPLACE retry behavior without allocating duplicate
entities. Relative operations still do not retry automatically, and unknown
append outcomes still stop retries. This is not cross-request deduplication:
a new client request has a new allocation lifecycle.

A missing source PK produces no Delete. An insert under its new AutoID therefore
writes only the destination vchannel; if later retried as an update, its Delete
and Insert both use that same AutoID. A batch can still span multiple vchannels,
whose transactions commit independently; there is no cross-vchannel atomicity.
After successful execution, Proxy returns destination IDs in input-row order,
while message packing continues to use the merged row order.

### CAS metadata and encryption

Proxy derives the final write vchannels from the current request PKs, including
allocated destination AutoIDs. It selects their proofs from those captured by
the single query; the missing source PK is not included in Delete or final CAS
solely because it was used for the initial lookup. CAS metadata contains only the attempt `readTS` and observed PChannel term; it does not
duplicate the collection ID, schema version, PK field ID, or PK list.

For every CAS Insert, StreamingNode:

1. reads `collection_id` and `schema_version` from the Insert header;
2. requires `schema_version` to be explicitly present;
3. resolves the immutable PK descriptor through ShardManager;
4. decodes the complete Insert body with the generated protobuf codec and
   extracts the descriptor's PK field and CAS metadata;
5. verifies that all CAS chunks in the transaction use the same proof,
   collection, and schema version.

Ordinary legacy Insert keeps its existing rolling-upgrade behavior and may
omit `schema_version`. Only CAS Insert requires an explicit version.

The message builder writes metadata into the Insert body before encryption and
before `BuildMutable()`. When cluster encryption is enabled, the proof is
inside the same encrypted boundary as the DML payload.

Insert and Delete tracking decode complete DML bodies and keep extracted PKs in
typed Int64/VarChar slices. This intentionally accepts the CPU, allocation, GC,
and append-latency cost of decoding unrelated fields, including vectors, to
keep protobuf wire compatibility owned by generated code instead of a custom
parser. If an encrypted payload cannot be decrypted, the current append returns
an error; the StreamingNode process does not panic.

The empty outer `_puc` marker only selects the transaction and lock paths.
After packing, Proxy verifies that:

- every prepared vchannel produced at least one CAS Insert;
- every CAS Insert carries the marker;
- every Insert vchannel belongs to the attempt snapshot;
- no final message exceeds the transport limit.

Missing metadata or a missing marker is an internal invariant violation. Proxy
does not rewrite an already constructed or encrypted body.

### Final-envelope packing

The existing entity-size packer does not include the later streaming header,
schema version, CAS metadata, outer properties, or encrypted envelope. An
entity-only-valid chunk can therefore exceed `pulsar.maxMessageSize` after final
construction.

CAS Insert uses two-stage packing:

1. Run the existing entity packer and retain the original row offsets for each
   chunk.
2. Add the streaming header, CAS metadata, and cipher through the message
   builder.
3. Check `EstimateSize()` on the final message.
4. If a multi-row message is oversized, bisect its contiguous row-offset range
   and rebuild both halves.
5. If a single-row message is still oversized, return
   `ErrParameterTooLarge` before WAL append.
6. Treat any oversized message that escapes this packer as an internal
   invariant violation.

Using contiguous original row-offset ranges preserves row order, field
alignment, partition, vchannel, and attempt metadata. Ordinary non-CAS Insert
keeps the existing packing path.

### Producer transaction packaging

`AppendMessages` groups DML by vchannel. A local group containing a CAS Insert
always uses a transaction, even if it contains only one Insert:

```text
BeginTxn
  -> transaction body
  -> CommitTxn with empty _puc marker
```

The producer does not rewrap a message that already has a `TxnContext` or
`ReplicateHeader`. Replicated messages preserve the source transaction
boundary.

The resumable producer immediately returns
`STREAMING_CODE_PARTIAL_UPDATE_RETRYABLE` to Proxy. It must not retry a
transaction that carries stale merged rows. If a local CAS transaction expires
before commit, the producer converts `TxnExpired` into the same CAS retry signal
so Proxy can rebuild the complete attempt.

Other transport failures keep the existing resumable-producer behavior.
BeginTxn, body, or CommitTxn can be retried after a stream failure, and the
final client outcome may be ambiguous. This proposal does not add
transaction-level idempotency.

### Interceptor ordering and row locks

The append chain is:

```text
redo -> lock -> replicate -> timetick -> shard -> partialupdate -> WAL
```

The lock interceptor retains the existing outer lifecycle boundary:

| Message | Lock |
|---|---|
| Ordinary DML, transaction body, ordinary or local CAS CommitTxn | `glock.RLock + vchannel.RLock` |
| Vchannel-exclusive DDL | `glock.RLock + vchannel.Lock` |
| PChannel-exclusive DDL | `glock.Lock` |

All non-PChannel-exclusive paths acquire the PChannel lock first and then the
vchannel lock. They release in reverse order.

A local CAS commit uses the same shared lifecycle locks as ordinary writes.
It does not call `FailTxnAtVChannel`; that cleanup remains associated with the
existing exclusive-DDL path.

The process-wide row-lock manager owns a fixed-size striped RWMutex table for
each active vchannel. A short manager mutex protects references only; no stripe
wait or WAL I/O holds it. There is no vchannel admission lock.

| Logical write | PK locks |
|---|---|
| Ordinary nontransactional Insert/Delete with exact PKs | Shared locks on the write set |
| Ordinary or replicated CommitTxn with reconstructed write set | Shared locks on the known prefix and suffix PK union |
| Local CAS CommitTxn with complete runtime proof | Exclusive locks on the complete PK union |
| Legacy Insert without schema version and without an available PK schema | None; preserve its original acceptance |
| TruncateCollection and exclusive DDL | Existing outer lifecycle lock only |
| BeginTxn, transaction body, RollbackTxn | None |

Only a version-less Insert with SchemaNotFound is exempt. Other schema errors,
missing descriptor getters, and malformed modern PKs retain their errors.
Legacy writes publish a collection timestamp fence after successful append,
but that record alone does not promise atomic CAS protection against legacy
writes. Mixed transactions lock their known PKs and publish both known PK
versions and any legacy fence. DDL retains its existing transaction cleanup.

Int64 and VarChar PKs use type-aware hash functions to select stripe IDs.
A fixed bitmap sorts and deduplicates those IDs before either lock mode acquires
them. Sorting PK values would be insufficient because hash order differs from
PK order. Deduplication also applies to distinct PKs that collide: recursively
acquiring a read lock can deadlock behind a queued writer. Single-PK messages
use the same hash and table with a direct one-lock path, avoiding the bitmap.

Ordinary writers share stripes; CAS waits for earlier shared holders and blocks
later overlapping holders until publication finishes. Exact PK versions remain
separate from lock stripes, so a hash collision adds waiting rather than a false
version conflict. Waiting can still consume the transaction lease. Large CAS
batches may occupy most stripes and block otherwise disjoint PK sets.

Reference counts include holders and waiters. Tables enter the pool only after
all acquired stripes have been unlocked and the last reference is released.
RWMutex waits are not cancellable; cancellation observed after acquisition
releases the acquired prefix and table reference. Close/Drop never reset active
tables.

The append chain keeps shard validation ahead of write tracking. During WAL
construction, `partialupdate.Build` registers its `MessageRowLock` through
the shared `InterceptorBuildParam.RowLock`. The earlier TimeTick builder
retains the shared parameter and reads this field when an append executes,
after all builders have completed. It therefore invokes the guard before final
timestamp allocation without moving the partial-update append interceptor
ahead of shard validation. The guard caches nontransactional write extraction
in the returned context so the tracking interceptor can reuse it after shard
validation; ordinary DML does not need to decode its payload twice.

The local CAS critical region is:

```text
acquire glock.RLock + vchannel.RLock
  -> replicate validation
  -> reserve provisional timestamp acker
  -> RequestCommitAndWait using provisional timestamp: freeze and drain body
  -> acknowledge provisional acker as sync
  -> snapshot complete durable write set
  -> hash PKs and acquire sorted, deduplicated exclusive stripes
  -> allocate final commitTS, respecting any timestamp barrier
  -> recheck transaction lease at final commitTS
  -> validate marker and runtime state
  -> validate term, read window, fences, and PK versions
  -> append CommitTxn
  -> publish PK / fence state
  -> CommitDone or RejectCommit
  -> acknowledge final message
  -> publish successful MVCC advancement
  -> release PK locks
release vchannel.RLock + glock.RUnlock
```

The provisional acker remains outstanding while a transaction freezes and drains
its bodies. Once preparation finishes, it is acknowledged before row-lock
admission so waiting commits do not pin the PChannel confirmation watermark.
TimeTick cleanup may expire a prepared transaction before or during lock
acquisition. The final timestamp lease check rejects it before WAL append,
even if row-lock acquisition has already copied its write set. Only the final timestamp
is assigned to the persisted CommitTxn. An error in guard acquisition, final
allocation, or the final lease check deterministically rejects a prepared
commit. Ambiguous WAL append failures retain their existing transaction
outcome handling.

Acknowledging the provisional timestamp is a sync-only acknowledgement: it
neither persists CommitTxn nor marks the transaction committed. Consumer
TxnBuffer still waits for the real commit, and LastConfirmedMessageID continues
to retain the WAL prefix needed by unresolved transactions. The eventual commit
receives a fresh timestamp after row-lock admission.

Ordinary nontransactional writes acquire shared locks for their exact PK set
before allocating the write timestamp and retain them through WAL append and
PK/fence publication. Ordinary and replicated transactions freeze and drain
their bodies before acquiring shared locks for the complete body PK union and
allocating commitTS. Local CAS commits acquire exclusive locks for that union.
Replicated CAS skips local proof revalidation but still participates in guard
and publication ordering. Therefore:

- a same-PK ordinary write admitted first completes publication before CAS
  obtains its exclusive guard and checks the snapshot;
- a later same-PK write waits for CAS publication and receives its visibility
  timestamp after CAS releases its guard;
- local CAS commits with overlapping stripes serialize; disjoint stripes in
  the same vchannel remain concurrent;
- ordinary writes to the same PK can proceed concurrently while no CAS writer
  is queued, because they hold shared PK locks; a queued CAS writer blocks new
  shared holders until it completes;
- recovered ordinary transactions lock the durable prefix plus runtime suffix;
  an unreconstructable modern write set requests existing transaction replay;
- different vchannels remain independent.

Acquiring PK locks only around `validateCommit -> WAL append -> publishCommit`
would be too late: the old TimeTick flow allocated commitTS before draining the
body and before entering the partial-update interceptor. A write could receive
an earlier timestamp, wait behind a later CAS, and escape that CAS's version
check. Acquiring the guard before the final timestamp prevents this inversion.

Proxy query and merge and transaction-body append remain outside PK exclusion.
Body messages stage writes and acquire no PK guards; only their complete commit
does. Otherwise a commit waiting for bodies could block a body needed to finish
its own write set. Guards remain held through publication, transaction
transition, acknowledgement, and successful MVCC advancement.

### Transaction state and atomic publication

The partial-update interceptor maintains `pendingTxn` in two phases for each
body message:

- before the inner append, it extracts and validates the attempt proof, derives
  collection and schema identity from the Insert header, resolves the PK
  descriptor from ShardManager, and registers the proof and derived scope. If
  any CAS chunk in the same transaction differs in proof, collection, or
  schema version, that body is rejected before it reaches the WAL backend;
- only after the inner append succeeds does it record whether the current
  interceptor lifecycle observed BeginTxn, the exact PK write set extracted
  from Insert and Delete, and an optional collection-wide fence.

Transaction bodies may append concurrently, so `pendingTxn` is protected by a
mutex. `RequestCommitAndWait` guarantees that no body remains in flight before
the pre-timestamp guard snapshots the write set and before commit validation.

Marker validation is fail closed:

- runtime CAS metadata without a local commit marker is unrecoverable;
- a marker with an observed BeginTxn but no valid proof, derived collection and
  schema scope, or PK write set is unrecoverable;
- a recovered local CAS that lacks a complete runtime proof is retryable and
  never reaches the WAL backend.

Commit admission uses the collection ID stored in `pendingTxn`, never a
collection ID supplied by CAS metadata, for collection-fence validation.

CAS validation runs before the inner CommitTxn append. A deterministic
admission reject is marked separately from a WAL append error:

- admission reject calls `RejectCommit()` and does not publish a write set;
- successful append publishes with the CommitTxn time tick before unlock;
- WAL append error does not publish, but `TxnSession` keeps the existing
  `CommitDone()` transition because the error cannot prove that the commit was
  not persisted.

This distinction changes only the in-process transaction transition. It does
not provide an exactly-once client outcome.

### Per-term PK version index

Each PChannel WAL term owns an independent registry, split by vchannel:

```text
registry[vchannel].pkLastWriteTS[pk] = lastCommitTS
```

The conflict rule is:

```text
conflict iff pkLastWriteTS[vchannel, pk] > readTS
```

The index supports Int64 and VarChar PKs. A newer write to an existing PK
updates the entry in place. The index never reuses state from an earlier WAL
term.

### Retention and memory bound

The PK index has a fixed 30-second TTL. All WAL terms on one StreamingNode
share the configured estimated-byte budget, while each WAL keeps independent
vchannel maps, expiration heaps, retention watermarks, and incomplete-history
markers.

An entry is charged conservatively:

```text
estimated bytes = 128 + VarChar PK bytes
```

The default 640,000,000-byte node budget is approximately five million Int64
entries across all WALs hosted by the StreamingNode.

`Update`, `Verify`, and TimeTick advancement incrementally evict expired
entries. Validation fails closed when:

- `readTS < retainedSinceTS`;
- physical read-to-commit age exceeds the TTL;
- `commitTS < readTS`, which is an unrecoverable internal invariant violation;
- the byte budget previously caused a committed write to be omitted.

If a new distinct PK cannot reserve its estimated bytes, ordinary writes remain
available, but the affected vchannel records `lastMissedWriteTS`. CAS on that
vchannel remains unavailable until the last missed write exits every valid
read window. Other vchannels are not directly failed by this state.

TimeTick advances retention even when no DML arrives, allowing idle channels to
release entries and recover from an incomplete window.

### Recovery and term changes

A newly opened WAL lifecycle starts with empty partial-update indexes and no
warm-up period. Admission requires readTS >= historyStartTs as well as a matching
term. A term change or a snapshot older than the new lifecycle floor rejects
the attempt; only eligible replacement updates are automatically retried.

TxnManager preserves existing TxnID, transaction recovery, and keepalive
behavior. During construction, partialupdate reads InitialRecoverSnapshot.TxnBuffer
and reconstructs the PKs of every durable transaction body. Later appends extend
this prefix. Commit freezes and drains the session before locking the union.

| Recovered path | Commit behavior |
|---|---|
| Ordinary transaction with reconstructable prefix | Continue old TxnID and lease; lock known PKs and publish versions/fences |
| Modern write set cannot be reconstructed | Existing TransactionExpired full producer replay; no new commit publication |
| Local CAS from an older WAL lifecycle | PartialUpdateRetryable; Proxy rebuilds the attempt |
| Replicated transaction with reconstructed prefix/proof | Preserve source-proof bypass; shared PK locks and publication |
| Already committed transaction | Preserve committed recovery and existing duplicate-result behavior |

Recovered pending state registers cleanup at the original last durable activity
TS. Recovery does not renew the lease, acquire or restore locks, change
recoveredSessions/RecoverDone, rebuild historical PK versions, or append body
messages again. Immutable messages use matching typed decoders. PK reconstruction
uses the WAL-open context with a five-second deadline; a failed decode marks only
that transaction incomplete, preserving durable data and WAL startup. The master
keyed-transaction path keeps its existing TransactionExpired fallback when its
buffered insert result is unavailable; this change adds no result-buffer restore.

### Replication

The primary already validated CAS. Replicated Commit does not revalidate source
term/readTS; it reconstructs the durable prefix, merges later body writes, takes
shared PK locks, and publishes versions. Unsupported modern prefixes request
existing full replay. Legacy schema-less writes retain the same explicit exemption.
A full CDC replay can reuse the source TxnID after an earlier Commit failed
before TimeTick acquired its transaction session, for example during the first
TSO allocation. Begin on the new session installs a fresh pending record before
registering cleanup: old PKs, CAS proof, recovery flags, and cleanup registration
cannot survive into this replay. Cleanup callbacks capture the record identity,
so a late callback from the superseded session cannot remove the new record.
Successful Begin marks this new record observed; failed Begin remains incomplete.
Ordinary resume without a new Begin preserves the recovered prefix and lease.

Promotion creates a new term and a new empty per-term version index.

### Error and retry semantics

| Origin | Internal classification | Proxy or client result |
|---|---|---|
| Term mismatch, PK conflict, collection fence, incomplete-txn fence, TTL expiry, or budget-incomplete history | `STREAMING_CODE_PARTIAL_UPDATE_RETRYABLE` | Rebuild `REPLACE`; project relative update to non-retriable conflict |
| Recovered local CAS without complete proof | `STREAMING_CODE_PARTIAL_UPDATE_RETRYABLE` | Same as above |
| Local CAS transaction expires before commit | Producer converts to `STREAMING_CODE_PARTIAL_UPDATE_RETRYABLE` | Same as above |
| Malformed marker, proof, Insert schema scope, PK write set, or internal invariant | `STREAMING_CODE_UNRECOVERABLE` | Fail without CAS retry |
| Shard schema version mismatch | `STREAMING_CODE_SCHEMA_VERSION_MISMATCH` | `ErrCollectionSchemaMismatch` |
| Timeout, disconnect, or unknown append result | Original transport or streaming error | Do not classify as deterministic CAS abort |

Proxy automatically retries only when every partial-update field operation is
`REPLACE`. Each retry:

1. restores the original partial field values, retaining any destination AutoIDs
   already allocated for this request;
2. regenerates function output;
3. determines all possible write channels;
4. resolves their terms before reading;
5. runs one Strong query and binds its actual per-channel snapshots; any
   newly missing original AutoID row receives a fresh ID without another read;
6. re-merges the original user payload with the new query results;
7. rebuilds Insert/Delete preprocessing and MutationResult counts;
8. accumulates storage cost from the new query;
9. creates new per-vchannel transactions.

The retry loop allows at most five attempts. Rebuilding the attempt with a
non-CAS error terminates the loop immediately.

Multi-vchannel responses are reduced as follows:

| Vchannel outcomes | Replacement update | Relative update |
|---|---|---|
| All success | Success | Success |
| Some success, remaining outcomes are deterministic CAS rejects | Rebuild and replay the complete request | Return non-retriable conflict |
| CAS reject mixed with timeout, unknown, or another non-CAS error | Return the non-CAS error; do not retry the request | Same |
| Only CAS rejects and retry budget is exhausted | Retriable service-unavailable | Non-retriable conflict |

Replaying a replacement is safe for already committed vchannels because it
reapplies absolute values after reading a newer snapshot. Relative operations
cannot use this rule because another vchannel may already have applied the
incremental change.

This response reduction does not create request-level atomicity. A client can
receive an error after one or more vchannel transactions have committed.

### Performance and capacity

The design adds the following costs:

- one query per partial-update attempt;
- complete protobuf decoding, PK extraction, and recent-version publication
  for ordinary Insert/Delete;
- an expiration-heap update for tracked PKs;
- hashing known PKs into a fixed bitmap and acquiring at most 4096 RWMutexes
  per write: shared for ordinary writes, exclusive for local CAS;
- contention between overlapping stripe sets, including hash collisions;
- PK locks retained across WAL I/O and acknowledgement/MVCC publication;
- a provisional acker and second timestamp allocation for each guarded
  CommitTxn, including ordinary and replicated transactions;
- decoding the durable prefix of pending transactions during WAL recovery;
- a final-envelope size check and possible CAS Insert repacking.

Different stripe sets remain concurrent within a vchannel. Different vchannels
use independent tables. Ordinary writes use shared PK locks and remain concurrent with
each other, including writes to the same PK when no CAS writer is waiting. Query, merge, and transaction-body append hold no PK locks. A slow WAL
commit blocks overlapping CAS/ordinary writes. Legacy unknown-PK writes have
no PK lock and do not introduce a whole-vchannel admission barrier. The existing shared shard
lifecycle lock remains held throughout append, so existing exclusive DDL
continues to wait for active writes.

Prepared commits waiting for row locks hold no outstanding provisional acker.
Unrelated acknowledged writes can advance the PChannel confirmation watermark
during that wait. An actual in-flight WAL append still holds its final acker and
can delay confirmation and consumer visibility across the PChannel. Benchmarks
should distinguish lock-queue delay from this WAL persistence delay and measure
both alongside write throughput and tail latency.

The required PK-index budget is approximately:

```text
required bytes ~= sum(128 + VarChar PK bytes)
                 for distinct PKs written within the TTL
```

With the proposal's budget and Int64 PKs, the index can retain about five
million distinct entries, equivalent to roughly 166,000 distinct PK writes per
second over a 30-second window.

Exceeding the budget reduces CAS availability for the affected vchannel but
does not reject ordinary writes.

Production-scale benchmarks are still required for:

- low-conflict CAS traffic;
- many disjoint PKs in one vchannel, including ordinary-write tail latency;
- high-conflict traffic on one PK or overlapping multi-PK batches;
- slow WAL append with disjoint and overlapping PK sets;
- transaction timestamp-allocation and provisional-acker overhead;
- shared PK lock acquisition and sorting overhead for ordinary writes without
  CAS, plus writer contention under mixed ordinary/CAS traffic;
- recovered ordinary transactions and legacy schema-less writes during ordinary/CAS load;
- high distinct-PK churn and long VarChar PKs;
- large batches near the transport message-size limit.

### Concurrency validation

Focused tests must verify ordering and observable concurrency, including:

- two disjoint stripe sets in the same vchannel can both enter WAL append
  while one remains blocked; an overlapping CAS waits, then observes
  the first commit's published PK version;
- a same-PK ordinary Insert/Delete or complete ordinary transaction excludes
  local CAS during its append/publication, while disjoint stripes proceed;
- ordinary writes hold shared PK locks and retain concurrent WAL append even
  for overlapping PKs; CAS waits for every earlier overlapping shared holder,
  and a queued CAS writer blocks new shared holders of those keys;
- both modes sort and deduplicate stripe IDs, including hash collisions while a
  CAS writer is waiting; cancellation observed after lock acquisition releases
  all partially acquired PK locks;
- a writer waiting behind a same-PK CAS receives its final timestamp only after
  acquiring the guard; commitTS allocated for provisional lifetime protection
  is replaced before persistence;
- CommitTxn guard selection occurs after all admitted body messages finish,
  uses the Insert/Delete union, and tolerates duplicate PKs and overlapping
  multi-PK requests with different input orders;
- Int64 and VarChar keys hash consistently; deliberate same-type and mixed-type
  collisions serialize safely, while exact PK version checks remain unchanged;
- recovered transactions lock prefix plus suffix without renewing the lease;
  schema-less legacy Inserts remain accepted without a row lock;
- a replicated write participates in the same stripes while retaining the
  source-proof bypass; an early TSO failure followed by same-TxnID full replay
  resets recovery state and PKs, and only the current record owns cleanup;
- the provisional acker protects body preparation, then is acknowledged before
  row-lock admission; queued commits at depths 1, 8, and 32 do not block
  confirmation of a disjoint acknowledged write once the active WAL append has
  completed;
- TimeTick cleanup can expire a prepared commit before its write-set snapshot
  or during a PK-lock wait; final timestamp lease validation rejects it
  before WAL append and releases its locks;
- guard failures reject prepared commits; successful guard release occurs only
  after transaction transition, final acknowledgement, and MVCC
  publication; ambiguous WAL errors keep their existing classification;
- exclusive DDL still excludes writes and performs its existing transaction
  cleanup, while local CAS never calls that cleanup path.

These checks establish synchronization behavior. They do not establish an
end-to-end throughput gain; production-scale benchmarks remain necessary.

## Open Questions

- Is the 30-second TTL sufficient for production query and commit latency?
- Is the proposal's five-million-entry budget sufficient for high-cardinality
  workloads and long VarChar PKs?
- How should Import, RestoreSnapshot, and backfill coordinate with partial
  update?
- Should Streaming transactions add a persistent request token to provide an
  exactly-once client outcome?


## Strong-read extension (2026-09-09, pending review)

### Read and commit contract

Every initial attempt and eligible retry uses the following Strong-read flow:

```text
resolve terms for all candidate write channels
  -> enqueue a Strong Query (GuaranteeTimestamp=0, MvccTimestamp=0)
  -> Query receives its own BeginTs from TSO
  -> each delegator waits for its guarantee and fixes its actual snapshot S[c]
  -> successful channel response reports S[c], including empty results
  -> Proxy validates all candidate write channels have nonzero S[c]
  -> allocate new PKs for missing AutoID rows without another query
  -> retain captured proofs for the final destination channels
  -> merge, then commit(term[c], readTS=S[c])
```

The outer Upsert BeginTs is not reused. This lets the existing Strong-read
optimization use the WAL MVCC barrier without waiting for a separately allocated
fixed read timestamp. QueryNode reports the request's executed MVCC snapshot,
not a later sample of tSafe. Internal `RetrieveResults.mvcc_timestamp` (field 20)
is an additive response field. Proxy's per-channel read timestamp map is separate from
query input overrides and is populated only by successful channel responses.
Different channels may use different snapshots; cross-channel atomicity is not
provided.

### WAL lifecycle history floor

Each partial-update interceptor records immutable `historyStartTs = F` from
`InterceptorBuildParam.LastTimeTickMessage.TimeTick()`. The WAL adaptor creates
this message by synchronizing TSO and durably appending the first TimeTick before
making the WAL available. The floor belongs to a WAL open lifecycle, even when a
WAL is reopened with the same PChannel term. It is not the recovered checkpoint
or a timestamp sampled at commit.

After the existing replication bypass and term validation, local CAS admission
requires `F != 0` and `readTS >= F`. An unavailable floor fails closed as an
internal unrecoverable error. A snapshot older than F returns the existing typed
partial-update retryable error before appending CommitTxn. The existing PK
conflict index, retention window, missed-write fence, and collection fence checks
still apply under the exclusive PK row locks and existing shared lifecycle
locks described above.

For example, B committed at 80 in the old lifecycle, and the new lifecycle starts
at F=90. Its index may not contain B. A read at S=70 is rejected even if its term
matches. A read at S>=90 includes B under the normal WAL visibility contract;
subsequent conflicting commits are covered by the current lifecycle's index.
Uncommitted ordinary transactions reconstruct known PKs and retain their
original session and lease. Recovered local CAS remains retryable.
Import, restore, and backfill remain subject to the original non-goals.

### Missing proofs and retries

A missing or zero read timestamp means a channel response cannot support CAS proof.
Proxy rejects the attempt before merge or any DML. Query errors propagate.
No compatibility fallback or feature flag is provided for old QueryNodes.

After a deterministic CAS rejection, existing eligible replacement updates
restore the original user fields and rebuild using a new Strong query.
Relative array operations keep their existing no-replay rule. Unknown commit
outcomes and mixed CAS/non-CAS failures do not become automatic retries.

Both first reads and retries start with zero internal MVCC and guarantee
placeholders, then let normal Strong query scheduling and execution determine
snapshots. No partial-update-specific read timestamp allocator or read-mode
switch remains. Ordinary non-partial query and Search requery timestamp
handling retain their existing contracts.

Strong snapshots remain subject to the existing 30-second CAS history window.
If a read is too old or the history index is incomplete, admission rejects it;
repeated rejections can exhaust the existing bounded retry budget. This design
does not force snapshot advancement with a fixed-timestamp fallback.

### Deployment assumption

All participating Proxies, QueryNodes, and StreamingNodes must run this protocol.
Every possible WAL owner must enforce the history floor, and every QueryNode
must report the executed snapshot. Mixed-version rollout and downgrade are
outside this design's scope; there is no capability negotiation or runtime
switch. QueryNode snapshot reporting alone does not prove WAL floor enforcement.
Reopens and frequent conflicts can increase Strong-query retries. Performance
improvement requires measurement and is not established by unit tests.
