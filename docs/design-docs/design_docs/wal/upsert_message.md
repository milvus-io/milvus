# Upsert WAL Message

## Scope

Upsert expresses replacement in one logical WAL message. It contains complete new rows and an unconditional set of primary keys (PKs) to delete. This capability is independent of PRIMARY_KEY indexing and does not change Proxy upsert production.

A producer must use stable PK-to-VChannel routing. One message targets one collection and one insert partition on one VChannel. Its delete scope covers all partitions of that collection on that VChannel. Cross-VChannel transactions and namespace-enabled collections are unsupported.

## Message contract

The V2 Upsert header contains collection identity, one PartitionSegmentAssignment, an explicitly present schema version, delete-row count, and optional IdempotentInsertResult metadata in idempotent_result (field 5). The body contains only InsertRequest (field 1) and DeleteRequest (field 2). The row payload follows body encryption. Response IDs and row offsets follow the existing Insert header boundary: the header is plaintext WAL metadata and can appear in message logs. PK values repeated in response metadata are outside payload encryption.

The distinct inserted and deleted PK sets must match exactly, using the schema's Int64 or VarChar PK type. Repeated PKs are permitted in either request. A repeated delete key cannot substitute for a missing key, and an extra delete key is invalid. Each request count must match its own payload. Collection IDs and the insert target partition must agree. The delete partition is AllPartitionsID.

InsertRequest.Version must be ColumnBased. Populated FieldsData does not override a RowBased format marker. As with Insert, producers must validate general field values and their physical representation before Produce. This includes sparse row encoding, vector dimensions, field lengths, nullable data, and recursive Array/VectorArray content. The Upsert handler checks the compound contract, not general field validity.

Producers supply canonical field payloads. Nullable scalar values and outer Array/VectorArray rows include one physical entry for every logical row. Nullable scalar Array elements are also expanded; ordinary nullable vector payloads retain their existing compact representation. Timestamptz uses TimestamptzData, and Geometry uses WKB GeometryData. The shard does not run Proxy input conversions or a separate general field validator. Raw Produce is an internal interface; malformed general fields violate its producer contract and can fail during consumption.

Producers supply final PKs and RowIDs. WAL does not allocate autoID or probe for old rows. A newly allocated key behaves like any other absent key. Public autoID upsert that deletes one key and inserts a different key remains on the existing Proxy path.

The shard checks the request schema version before it checks the compound contract against the current schema. A stale request returns SchemaVersionMismatch so the producer can invalidate its schema cache and rebuild the request. Same-version violations of the compound contract remain terminal errors. General field validity remains the producer responsibility. The shard materializes function outputs under the existing function policy. It assigns the insert segment locally. Nested timestamps, segment IDs, and shard names do not define visibility or cross-cluster physical identity.

## Visibility

The final visibility TimeTick is the autocommit Upsert timestamp or its enclosing transaction's CommitTxn timestamp.

Consumers insert new rows at that timestamp. They delete matching versions whose insert timestamps are strictly earlier. Equality protects the new rows from their companion deletion. Missing old keys do not change the operation.

An autocommit Upsert has one MessageID and normal AppendResult. It does not expand into an internal transaction. Backend chunking can split bytes but does not create separately acknowledged Insert/Delete operations.

## Transactions

Upsert is one logical body of an existing single-VChannel transaction. Commit requires an exact, nonzero body count. After scope and body identity checks, a nonzero count mismatch returns TransactionExpired and ends the attempt so the producer can rebuild it with a new TxnID. This also covers body retransmission after a lost acknowledgment. Zero count remains a terminal error. Uncommitted, rolled-back, expired, or exclusively aborted bodies produce no visible effect.

Uniform Commit timestamps do not express body-order replacement. Repeated PKs within one Upsert and overlaps among Upsert, Insert, and Delete bodies are accepted. These bodies use the existing transaction semantics at one TimeTick. This protocol does not promise last-write-wins for equal-TimeTick versions. Deduplication, merging, and overwrite policy belong to future pkindex work. Transactions without Upsert retain their existing semantics.

An Upsert body contains every intended delete key even if no old row exists when it arrives. If another writer commits that key before the transaction commits, the later transaction deletes the intervening version. Rollback preserves the intervening version.

### Scope enforcement

TxnID lookup alone does not establish scope. Before admitting an Upsert body, TimeTick compares its VChannel with the Begin session. Shard checks the canonical collection identity against the header, nested requests, and its authoritative collection-to-VChannel binding.

After RequestCommitAndWait freezes the successful bodies, the transaction layer checks Begin, all bodies, and Commit for one VChannel and collection. This includes legacy Insert/Delete bodies before or after Upsert. TimeTick invokes this validation before Commit persistence and before shard dispatch. Shard registers Upsert in its existing operation table and always dispatches Upsert, including control and empty scopes, so the handler can reject them. Shard does not register a CommitTxn operation.

A rejected pre-admission body changes no successful count or lease. A rejected Commit uses marked admission rejection and the existing RejectCommit cleanup. Recovery restores complete body references and applies the same checks before a new Commit can persist. It never drops a foreign body to make a prefix appear complete.

Session references do not retain persistence Ack handles. They clear on commit, rollback, expiry, abort, recovery failure, and shutdown. Legacy transactions also retain Begin and successful-body references because Upsert can arrive after a legacy body. The reference slice grows with the successful body count and retains those payloads until cleanup. This change preserves those references for scope validation, transaction validation, and recovery. It adds no overlap map. The existing memory measurement is not a bound for arbitrary payload sizes or transaction lengths.

## Retry results

The existing local idempotency window bounds retry suppression. A keyless retry can create another version. This protocol does not add an unlimited or cross-cluster exactly-once guarantee.

Upsert uses the same optional idempotent_result header field and result format as Insert. An omitted field is valid; a supplied field must have valid IDs and row offsets. The keyed retry path returns the original AppendResult. It does not substitute IDs from a retry. As for Insert, an autocommit result header is paired with an idempotency key. Transaction bodies may carry metadata for their enclosing keyed Commit. Producers supply row offsets that identify distinct positions in the logical request.

The producer contract is: Begin succeeds, all bodies are sent, all body appends succeed, then Commit is sent. This protocol adds no support for Commit sent before body responses complete. The original TimeTick RequestCommitAndWait remains unchanged.

The idempotency interceptor records each successful body's result in its existing transaction result buffer. Commit uses that buffer and the existing MergeIdempotentInsertResults path. Missing or incomplete results return TransactionExpired before Commit append, so the existing producer rebuilds the transaction. One shared completeness check covers Insert-only and mixed transactions. The buffer records missing or incomplete metadata and never caches only the remaining bodies' results. This adds no Upsert-specific mandatory field: keyless writes can omit metadata, and keyed transactions use the existing TransactionExpired path when a complete result cannot be built.

The common idempotency response helper supplies the buffered result to the original success and duplicate response. It preserves an Extra already supplied by the append path. There is no result merge, schema dependency, or result callback in shard or TimeTick. Result extraction in idempotency and Summary reads headers without decoding or decrypting the Upsert body. Summary still decodes the compound body for its Delete effect and preserves its atomic failure behavior.

After WAL reopen, Summary facts restore committed duplicate responses. A pending transaction whose volatile result buffer was lost uses the existing TransactionExpired and whole-transaction rebuild path. This change does not reconstruct results after waiting for bodies at Commit. Existing rollback, expiry, and abort paths release rejected attempts.

## Consumption and durability

Consumers use read-only projections of the original Upsert. They do not construct independent WAL Insert/Delete messages. A consumer must copy request metadata before rewriting timestamps or assignments because immutable bodies share cached decoded data.

RecoveryStorage owns one outer Upsert or committed Txn. SegmentView retains that outer owner for L1 rows. WALMaterializer retains it for L0 deletes. Both effects and Summary coverage must complete before checkpoint publication. L1 and L0 component cursors remain independent during replay.

Summary records both write facts and delete blocks at the final timestamp. Native growing runtimes apply both effects before advancing query frontiers. IDF receives the inserted rows through the existing insert walker.

Legacy MsgPack consumers are outside this feature's supported paths. Native consumers decode the complete compound content before applying either effect or advancing their frontiers.

## CDC

Upsert requires a persisted, replicable, single-VChannel envelope without broadcast metadata. It rejects the unreplicable, nonpersisted, and PChannel-level markers. A transaction containing Upsert applies this rule to Begin, every successful body, and Commit, including recovered bodies. Legacy-only transactions keep their existing contract. The shard handler checks the envelope before business effects. Idempotency cache hits and replicated duplicate/ignore classification can return before shard; those paths check the envelope before returning a cached or ignored result. CDC forwards its explicit content and source logical identity. Secondary assigns its own segments and local visibility timestamp. It does not query a local pkindex to determine deletes.

Transaction replay preserves one Upsert body, source-ID deduplication, and the original exact Commit count. Receiving prefixes recover through the existing TxnBuffer. Switchover depends on durable row/delete state rather than primary-local index state.

## Compatibility and activation

`streaming.wal.upsert.enabled` defaults to false. The append adaptor checks it before interceptor mutation for local, remote, and replicated appends. Readers never consult this generation gate. Existing durable bodies can still Commit or Rollback while generation is disabled.

The default remains false. An explicit true bypasses version checks. With auto configured, the existing VersionGateSwitcher keeps generation disabled until the local cluster's minimum online session version reaches 3.0.7 and remains there for one minute, then publishes true through the config center. The item is registered with the existing coordinator switcher; no separate polling service is added.

Configure auto before coordinator startup. The switcher is one-shot: it does not re-arm a gate after resolving an explicit false value or after enabling it. A later change from false to auto requires restarting the coordinator to start another confirmation. Explicit true/false values remain refreshable. Embedded-etcd single-process deployments use the existing local-version resolution.

The control plane guarantees that CDC secondary cluster versions lead the primary. Configure the receiving clusters' gates as well as the source gate; the local switcher does not inspect remote clusters or enable their configs. No cross-cluster capability negotiation is added. Once enabled, the switcher does not continuously reject an older node joining later and does not guarantee downgrade compatibility.

Old Produce ingress rejects unknown numeric types. Other old paths can pass them through, miss their effects, or panic during conversion. Ignoring unknown messages is unsafe.

Turning generation off does not remove retained Upsert history. Upgraded readers remain necessary for reopen, reassignment, CDC backlog, backups, and restore. This feature supplies no WAL rewrite or downgrade tool.

Existing Proxy upsert retains its Insert/Delete transaction path. Explicit native callers receive a terminal error when generation is disabled. No shard-level transparent fallback is supplied.

## Validation obligations

Verification must inspect old and new row content, not only PK counts. Cover autocommit, concurrency, transaction rejection, recovery, chunking, response loss, independent L1/L0 persistence, flush/reload, compaction, function outputs, and cipher failures.

Use a real Produce RPC integration helper for the native type. CDC validation requires two clusters, receiving-prefix replay, independent physical assignments, and switchover. Record unexecuted scenarios explicitly. Unit conversion tests do not establish data-level correctness.

## Existing documentation

Production opener, VChannel observation, and query resource construction connect the native query path. This change updates the earlier descriptions of QueryRuntime forwarding as future work. The active retained-WAL L0 implementation remains distinct from the dormant Summary-reader L0 implementation.

## Validation boundaries

The tests check compound PK set equality with repeated Int64 and VarChar keys, transaction scope and exact counts, normal-order buffered response completeness, recovery, cleanup, and complete Insert/Delete effects. Tests for general field rejection and PK overlap rejection are removed because those are no longer server admission policies.

Valid sparse vectors and BM25 function output remain test cases. MinHash output is not verified. Invalid general field payloads are not tested through raw Produce because the producer must reject them before that interface. The generation gate does not establish automatic mixed-version safety or a downgrade guarantee. Test execution and remaining limits are recorded in the external verification report.

## Draft field removal and generation

Upsert is not released. The former UpsertMessageBody.append_result field 3 is removed, with its number and name reserved. There is no legacy body fallback and no migration that copies an unknown body field into the header. Test builders and fixtures use the header as their only result source. WAL data produced by earlier drafts of this PR is not a supported retained-history format; recreate those development fixtures.

Insert/Delete and all other existing message enum values and field numbers remain unchanged. Upsert keeps its existing header fields 1 through 4 and adds idempotent_result at field 5. Protocol tests check optional presence and removal of the body accessor. Protobuf and message helpers are regenerated through the repository targets and checked for repeatability.

## Query path dependency

Deployment of native Upsert depends on the query-path transition in [QueryView PR #52977](https://github.com/milvus-io/milvus/pull/52977). This feature does not add Upsert adaptation to the legacy MsgPack pipeline or msgdispatcher. It does not support native Upsert with that legacy query path.

The dependency's startup commit 475778acf1 is not an ancestor of this branch. The generation gate must remain disabled for deployments that still use the legacy query path. The version gate alone does not verify this dependency. This PR does not rebase or import the dependency implicitly. Query integration must be verified on the combined revision before activation.

Existing MsgStream, SimpleMsgDispatcher, and flowgraph tests remain unchanged. SegmentInsertMessage retains its InsertRequest interface because native growing runtime, IDF, and WALView materialization use it to read nested Upsert rows without creating a synthetic top-level Insert message.
