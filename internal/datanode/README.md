# Data Node

DataNode is the component to write insert and delete messages into persistent blob storage, for example minIO or S3.

## Dependency

- KV store: a kv store that persists messages into blob storage.
- Message stream: receive messages and publish imformation
- Root Coordinator: get the latest unique IDs.
- Data Coordinator: get the flush information and which message stream to subscribe.
