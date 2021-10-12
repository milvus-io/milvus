# Data Coordinator

Data cooridnator(datacoord for short) is the component to organize DataNodes and segments allocations.

## Dependency

- KV store: a kv store has all the meta info datacoord needs to operate. (ETCD)
- Message stream: a message stream to communicate statistics information with data nodes. (Pulsar)
- Root Coordinator: timestamp, id and meta source.
- Data Node(s): could be an instance or a cluster, actual worker group handles data modification operations.
