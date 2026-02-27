# StreamingClient

In-process singleton library (accessed via `streaming.WAL()`) providing the client-side API for WAL operations: Append, Read, Broadcast, and Txn.

Append and Read route to the StreamingNode owning the target PChannel via [Channel Management](../coordination/channel_management.md) service discovery, and auto-recover from StreamingNode failures. When the PChannel is assigned to the local StreamingNode (same process), they bypass gRPC and operate directly on the local WAL instance. Broadcast delegates to StreamingCoord's [Broadcaster](../coordination/broadcaster.md) via gRPC.

## Key Packages

- `internal/distributed/streaming/` — `WALAccesser` singleton, producer, consumer
- `internal/streamingnode/client/` — `HandlerClient`, local/remote dispatch
