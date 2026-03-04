# Lock

The Lock interceptor enforces exclusive/shared access on VChannel or PChannel scope during append, preventing concurrent conflicting operations.

## Lock Levels

- **PChannel-level**: A global RWMutex. Exclusive lock blocks all appends on the entire PChannel; shared lock allows concurrent appends.
- **VChannel-level**: A per-key RWMutex. Exclusive lock blocks appends on a specific VChannel; shared lock allows concurrent appends to the same VChannel.

See [Message Semantic Docs](../message/message.md) for per-message lock requirements.

## Key Packages

- `internal/streamingnode/server/wal/interceptors/lock/` — `lockAppendInterceptor`, builder
