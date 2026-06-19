# Planned Switchover

Planned switchover changes the primary-standby direction without data loss. Use it when the current primary cluster is still reachable, or when you need to move traffic for maintenance.

This guide assumes the current topology is:

```text
cluster-a (primary)  ->  cluster-b (standby)
```

After switchover, the topology becomes:

```text
cluster-b (primary)  ->  cluster-a (standby)
```

## When to Use Planned Switchover

Use planned switchover when:

- You are doing maintenance on the current primary.
- The primary is partially degraded but can still respond to requests.
- You need RPO = 0 and cannot accept data loss.

Do not use planned switchover if the primary is completely unavailable. In that case, use [Force Failover](./04-cdc-force-failover.md).

## Before You Begin

Check the following before starting:

- Both clusters are reachable.
- CDC replication is healthy.
- CDC lag is low enough for your recovery time target.
- Application writes can be paused or retried during the role change.
- You have prepared the new topology configuration.

Planned switchover guarantees no data loss, but the operation time depends on how much data remains to be replicated.

## Build the New Topology

Create a full replacement configuration where `cluster-b` becomes the source and `cluster-a` becomes the target.

```python
# If you followed the Quick Start, cluster A is the original source cluster,
# and cluster B is the original target cluster.
cluster_a_id = source_cluster_id
cluster_a_addr = source_cluster_addr
cluster_a_client_addr = source_client_addr
cluster_a_token = source_cluster_token
cluster_a_pchannels = source_cluster_pchannels

cluster_b_id = target_cluster_id
cluster_b_addr = target_cluster_addr
cluster_b_client_addr = target_client_addr
cluster_b_token = target_cluster_token
cluster_b_pchannels = target_cluster_pchannels

switchover_config = {
    "clusters": [
        {
            "cluster_id": cluster_a_id,
            "connection_param": {
                "uri": cluster_a_addr,
                "token": cluster_a_token,
            },
            "pchannels": cluster_a_pchannels,
        },
        {
            "cluster_id": cluster_b_id,
            "connection_param": {
                "uri": cluster_b_addr,
                "token": cluster_b_token,
            },
            "pchannels": cluster_b_pchannels,
        },
    ],
    "cross_cluster_topology": [
        {
            "source_cluster_id": cluster_b_id,
            "target_cluster_id": cluster_a_id,
        }
    ],
}
```

## Apply the New Topology

Apply the same configuration to both clusters. Send the request to the current primary first, and then send it to the standby. If you later switch back, reverse the order because `cluster-b` is the current primary.

```python
from pymilvus import MilvusClient

client_a = MilvusClient(uri=cluster_a_client_addr, token=cluster_a_token)
client_b = MilvusClient(uri=cluster_b_client_addr, token=cluster_b_token)

try:
    client_a.update_replicate_configuration(**switchover_config)
    client_b.update_replicate_configuration(**switchover_config)
finally:
    client_a.close()
    client_b.close()
```

The old primary demotes to standby and rejects new writes. The old standby waits for remaining replicated data, promotes itself to primary, and then accepts writes.

If the request fails because of a transient network or service error, retry with the same configuration.

## Redirect Application Traffic

After `cluster-b` becomes primary:

1. Point write traffic to `cluster-b`.
2. Confirm reads and writes succeed on `cluster-b`.
3. Confirm `cluster-a` is no longer receiving application writes.
4. Keep monitoring replication from `cluster-b` back to `cluster-a`.

## Verify the Result

Verify that `cluster-b` is serving as the new primary and that data remains consistent. Common checks include:

- Compare row counts for important collections.
- Query known primary keys from both clusters.
- Run a representative search on the new primary and old primary.
- Run a small write on `cluster-b` and confirm it is replicated to `cluster-a`.

## Switch Back

To switch back later, apply the original topology again:

```text
cluster-a -> cluster-b
```

Use the same planned switchover flow. Make sure the current primary is reachable and replication is healthy before switching back.

## FAQ

### Does planned switchover lose data?

No. Planned switchover waits for remaining data to be replicated before the standby becomes primary.

### Do I need to stop application writes?

You should pause writes or make writes retryable during the role change. Writes sent to the old primary after it demotes are rejected.

### Why does switchover take longer than expected?

The most common reason is CDC lag. The new primary must receive remaining data before it can safely take over with RPO = 0.

### Can I retry a failed switchover request?

Yes. Retry with the same target topology.

### What happens to the old primary?

The old primary becomes a standby. It should no longer receive application writes.
