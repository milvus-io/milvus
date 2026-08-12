# Force Failover

Force failover promotes a standby cluster to a standalone primary when the original primary is completely unavailable. It is an availability-first operation and may lose data that was not replicated before the failure.

This guide assumes the original topology is:

```text
cluster-a (primary)  ->  cluster-b (standby)
```

After force failover, `cluster-b` becomes a standalone primary:

```text
cluster-b (primary)
```

## When to Use Force Failover

Use force failover only when:

- The original primary cannot respond to requests.
- The primary cannot be recovered within an acceptable time.
- Restoring write availability is more important than waiting for the old primary.

If the primary is still reachable, use [Planned Switchover](./03-cdc-planned-switchover.md) instead. Planned switchover avoids data loss.

## Data Loss Risk

Force failover does not wait for the original primary. Any data written to the old primary but not yet replicated to the standby may be lost.

The possible data loss is determined by CDC lag at the time the primary became unavailable.

Before running force failover, understand the tradeoff:

| Goal | Planned switchover | Force failover |
|---|---|---|
| Restore writes while primary is unreachable | No | Yes |
| Avoid data loss | Yes | Not guaranteed |
| Requires old primary to respond | Yes | No |

## Before You Begin

Confirm the following:

- The original primary is unavailable.
- You have decided not to wait for primary recovery.
- Application traffic can be redirected to the standby.
- Traffic automation will not send writes back to the old primary if it recovers.
- You have the standby cluster ID, address, token, and pchannels.

The most important safety requirement is to prevent split brain. After force failover, only the promoted standby should accept application writes.

## Build the Force Failover Configuration

Build a configuration that contains only the standby cluster and no replication topology. Set `force_promote=True`.

```python
# If you followed the Quick Start, cluster B is the original target cluster.
cluster_b_id = target_cluster_id
cluster_b_addr = target_cluster_addr
cluster_b_client_addr = target_client_addr
cluster_b_token = target_cluster_token
cluster_b_pchannels = target_cluster_pchannels

force_failover_config = {
    "clusters": [
        {
            "cluster_id": cluster_b_id,
            "connection_param": {
                "uri": cluster_b_addr,
                "token": cluster_b_token,
            },
            "pchannels": cluster_b_pchannels,
        }
    ],
    "cross_cluster_topology": [],
    "force_promote": True,
}
```

## Promote the Standby

Send the request to the standby cluster.

```python
from pymilvus import MilvusClient

client_b = MilvusClient(uri=cluster_b_client_addr, token=cluster_b_token)

try:
    client_b.update_replicate_configuration(**force_failover_config)
finally:
    client_b.close()
```

If the request succeeds, `cluster-b` becomes a standalone primary and can accept writes.

## Redirect Application Traffic

After promotion:

1. Redirect write traffic to `cluster-b`.
2. Remove `cluster-a` from write endpoints, load balancers, DNS records, and automation.
3. Verify that `cluster-b` accepts writes.
4. Keep `cluster-a` isolated until it is decommissioned.

Example write verification:

```python
client_b = MilvusClient(uri=cluster_b_client_addr, token=cluster_b_token)

try:
    client_b.insert(
        collection_name="test_collection",
        data=[{"id": 1, "vector": [0.1] * 128}],
    )
finally:
    client_b.close()
```

Adjust the collection name and schema fields to match your deployment.

## Verify the Result

Verify the promoted cluster directly:

- Writes succeed on `cluster-b`.
- Reads return expected data.
- No application component writes to `cluster-a`.

## Handling the Old Primary

After force failover, decommission `cluster-a`. Do not send application writes to it if it becomes reachable again. It may contain data that was never replicated to `cluster-b`, and `cluster-b` may already contain new writes after failover.

Do not reconnect `cluster-a` to the old topology.

## Minimizing Data Loss

You cannot remove all data-loss risk from force failover, but you can reduce it:

- Monitor CDC lag continuously.
- Keep standby clusters provisioned to handle the primary write rate.
- Keep cross-cluster network latency and packet loss low.
- Make application writes idempotent.
- Retry writes whose success is uncertain after failover.
- Prefer planned switchover whenever the primary can still respond.

## FAQ

### Does force failover always lose data?

No, but it can. If all writes were already replicated before the primary failed, no data is lost. If CDC lag existed, the lagging data may be lost.

### How long does force failover take?

It typically completes within seconds, depending on cluster state and control-plane availability on the standby.

### Can I run force failover on the primary?

No. Force failover is intended for a standby cluster. If the current primary is available, use planned switchover.

### Can the old primary rejoin automatically?

No. After force failover, the old primary must be treated as stale and decommissioned.

### How do I avoid split brain?

Ensure that only the promoted cluster receives writes. Remove the old primary from all write paths before it can recover and accept traffic.
