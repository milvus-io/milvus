# WAL payload chunking

## Scope

StreamingNode (SN) splits oversized encoded logical messages into physical WAL records below the interceptor chain. A reader reconstructs the complete message before interpreting its body. The mechanism applies to all supported message types, including Insert V2, Upsert, and transaction bodies.

3.1 uses this path without activation settings. Proxy does not split batches to satisfy backend record limits. It creates one logical message per nonempty VChannel group. Several partitions can share that message through the [Insert V2/Upsert row format](wal/upsert_message.md). Local partial-update CAS still uses its existing transaction wrapper; chunking does not change that protocol.

## Physical contract

The final payload is opaque bytes, possibly already encrypted. `SplitIntoChunks` slices those bytes without protobuf decode or row copying. Each chunk keeps the original message type and a clone of all properties, plus `_ci` (zero-based index) and `_ct` (total). Backend delivery filters must deliver every chunk wherever they would deliver the complete message.

All chunks retain one local TimeTick. Each physical record has its backend ID. The logical message ID returned by append is the successful append attempt's chunk-0 ID. Append succeeds only after every chunk is durable. Chunks append sequentially within a run, but concurrent logical messages can interleave without a new write lock.

A chunk is not a valid standalone body. The Produce ingress keeps its existing restriction on caller-supplied `_ci`/`_ct`; only the storage-layer writer creates them.

## Write path and budget

`walAdaptorImpl.Append` runs the interceptor chain, then `appendWithOptionalChunking` uses the backend budget. Despite that helper's historical name, chunking is unconditional when the payload exceeds the positive budget. Fitting payloads and backends without a budget use one record.

`WALMaxMessageSize` selects the effective limit for Pulsar or Kafka and the configured chunk threshold for Woodpecker. RocksMQ has no chunk budget. Numeric bounded limits are normalized to at least 256 KiB. External broker/topic limits must support the effective Milvus configuration; local normalization cannot raise an external limit.

The payload budget subtracts `pulsar.messageReserveSize`, shared across backends despite the configuration name. It defaults to 64 KiB and covers properties and backend envelope. A valid reserve is at least 1 KiB and below the active limit. Invalid reserve values use the existing normalized fallback. The budget cache refreshes within one second.

The backend retry loop retains the same TimeTick and chunk bytes until success or writer shutdown/fencing. It does not shrink an already-retrying record after a live configuration change. A misconfigured external cap can therefore still cause retries. No new retry policy is part of chunking.

## Reading, retries and barriers

Durable catchup feeds physical records to `ChunkAssembler` before v0 conversion, filtering, reordering and transaction assembly. WAB tailing already supplies complete logical messages.

The assembler groups runs by local TimeTick and stores sparse index slots. It concatenates complete runs, removes chunk markers, and returns chunk 0's properties and MessageID. A payload-identical repeat of one slot replaces that slot with the later observation. This preserves the successful retry's head ID after an earlier attempt persisted but lost its ACK.

Malformed markers, differing bytes for one slot, or inconsistent totals return corruption errors. The scanner stops instead of retrying through corruption. The recovery stream marks its WAL unavailable on a fatal reader failure. It cannot publish completion past the failed operation.

Incomplete runs never reach body consumers. A TimeTick or MVCC barrier discards incomplete runs at or below its timestamp. The ACK watermark makes this safe: a barrier cannot pass a still-live earlier logical append whose chunks have not all completed. Converted legacy TimeTicks also advance assembler cleanup. An orphan middle chunk with no observed head is swallowed, as a scan may start within a physical run.

No reader-only count/byte eviction limit silently drops runs. A limit cannot distinguish a dead writer from a live interleaved append, and dropping the latter can lose acknowledged data. Partial runs are released at a safe barrier or scanner teardown.

## Logical identity and consumers

Physical chunks count as one logical transaction body. They do not add Begin/Commit controls. A transaction's final visibility still comes from Commit, independently of the local body TimeTick used for chunk grouping.

Consumers receive complete messages. Chunking preserves logical visibility but does not add rollback around native apply. Actual decrypt, decode, conversion and apply errors retain their existing error/frontier behavior.

CDC reads reconstructed logical messages. The secondary preserves logical rows/effects and assigns its own segment placement and physical chunk boundaries. Primary and secondary do not need identical backend budgets.

## Memory and request bounds

Chunk slicing borrows the encoded source payload. Durable assembly retains chunk payloads and allocates one contiguous concatenation buffer, approximately two payload copies before decryption and downstream decoding. Several scanners or interleaved runs can multiply that memory.

Proxy retains materialized Insert/Delete request limits and transport limits. Those application bounds do not include every later function output or envelope byte and are not an exact final-payload bound. Physical chunking cannot justify unlimited logical writes. Payload pooling is outside this mechanism.

## Upgrade and compatibility

3.1 removes `proxy.splitChunk` and `streaming.splitChunkSN`. The previous dual-switch rollout is not the 3.1 contract. 3.1 Proxy sends logical batches and SN always chunks when needed. Readers always reassemble.

All components first reach a prerequisite latest 3.0 release with complete new-format receiving/consumption and chunk-reading support. That compatibility backport is separate release work. Existing 3.0 producers and rollout controls are not changed by this document.

During mixed-version upgrade a new Proxy may send a large message to an old SN that does not chunk writes. It can remain in that owner's backend retry loop. After the owner shuts down or is fenced and reassigned, a still-live client operation can resume on a new owner. The request deadline can expire first. This temporary delay or timeout is accepted; no new fallback, version probe or retry mechanism is added.

Deployment management must ensure CDC targets understand new logical formats and retained chunk history. A binary without chunk assembly cannot read retained chunks. Removing write switches does not establish downgrade safety.

## Validation requirements

Verify exact boundaries, fitting/empty payloads, unbounded backends, interleaved runs, all required properties, payload byte parity, successful-head identity after lost ACK, malformed/conflicting chunks, sparse slot allocation, orphan cleanup and durable/WAB switching.

Use real WAL tests for chunked Insert V2/Upsert, encrypted payloads, keyed retries, CAS transaction bodies, partial writes, restart and persistence. Inspect row values and original results, not only append success. Verify corruption prevents completion/frontier advancement.

Run a real prerequisite-3.0/3.1 owner-transition test for the accepted upgrade window. A mocked producer swap is not rollout evidence. Measure memory/allocations and physical bytes before claiming performance gains.
