# CDC Replication Quick Start

This guide shows how to deploy two standalone Milvus clusters with Milvus Operator and configure CDC replication from a source cluster to a target cluster.

The examples use:

- `source-cluster` as the primary cluster.
- `target-cluster` as the standby cluster.
- `milvus` as the namespace for Milvus clusters.
- `milvus-operator` as the namespace for Milvus Operator.

Before you begin, read [CDC Replication Overview](./01-cdc-replication-overview.md) to understand the primary-standby model and failover options.

## Prerequisites

- Milvus v2.6.16 or later.
- Milvus Operator v1.3.4 or later.
- A Kubernetes cluster is available.
- The source and target clusters can connect to each other over the network.
- You have admin credentials for both Milvus clusters.
- You know the physical channel count for each cluster.

## Step 1: Upgrade Milvus Operator

Add the Milvus Operator Helm repository:

```bash
helm repo add zilliztech-milvus-operator https://zilliztech.github.io/milvus-operator/
```

Update the repository:

```bash
helm repo update zilliztech-milvus-operator
```

Install or upgrade Milvus Operator:

```bash
helm -n milvus-operator upgrade --install milvus-operator \
  zilliztech-milvus-operator/milvus-operator \
  --create-namespace
```

Check that the operator pod is running:

```bash
kubectl get pods -n milvus-operator
```

Example output:

```text
NAME                               READY   STATUS    RESTARTS   AGE
milvus-operator-6f7d8c9c7d-xm4tj   1/1     Running   0          54s
```

## Step 2: Deploy the Source Cluster

Create a file named `milvus_source_cluster.yaml`:

```yaml
apiVersion: milvus.io/v1beta1
kind: Milvus
metadata:
  name: source-cluster
  namespace: milvus
  labels:
    app: milvus
spec:
  mode: standalone
  components:
    image: milvusdb/milvus:v2.6.16
    cdc:
      replicas: 1
  dependencies:
    msgStreamType: woodpecker
```

Apply the configuration:

```bash
kubectl create namespace milvus
kubectl apply -f milvus_source_cluster.yaml
```

Check that the source cluster pods are running:

```bash
kubectl get pods -n milvus
```

Example output:

```text
NAME                                                   READY   STATUS    RESTARTS   AGE
source-cluster-etcd-0                                  1/1     Running   0          3m
source-cluster-minio-6d8f7d9b9f-9t7j2                  1/1     Running   0          3m
source-cluster-milvus-standalone-7f8d9c8f6d-r2m5x      1/1     Running   0          2m
source-cluster-milvus-cdc-66d64747bd-sckxj             1/1     Running   0          2m
```

Make sure the CDC pod, such as `source-cluster-milvus-cdc-...`, is in the `Running` state.

## Step 3: Deploy the Target Cluster

Create a file named `milvus_target_cluster.yaml`:

```yaml
apiVersion: milvus.io/v1beta1
kind: Milvus
metadata:
  name: target-cluster
  namespace: milvus
  labels:
    app: milvus
spec:
  mode: standalone
  components:
    image: milvusdb/milvus:v2.6.16
    cdc:
      replicas: 1
  dependencies:
    msgStreamType: woodpecker
```

The CDC component is enabled on the target cluster as well. It is idle while the target is a standby, but it is needed if the target later becomes the primary after planned switchover.

Apply the configuration:

```bash
kubectl apply -f milvus_target_cluster.yaml
```

Check that the target cluster pods are running:

```bash
kubectl get pods -n milvus | grep -E 'NAME|target-cluster'
```

Example output:

```text
NAME                                                   READY   STATUS    RESTARTS   AGE
target-cluster-etcd-0                                  1/1     Running   0          3m
target-cluster-minio-5f7c8d9b6f-k8s2q                  1/1     Running   0          3m
target-cluster-milvus-standalone-66dc8d9f7f-5n6bp      1/1     Running   0          2m
target-cluster-milvus-cdc-7f8c9d6b8c-q4t9m             1/1     Running   0          2m
```

## Step 4: Prepare Cluster Information

Get the Milvus service addresses for both clusters:

```bash
kubectl get svc -n milvus | grep -E 'NAME|source-cluster|target-cluster'
```

Example output:

```text
NAME                                  TYPE        CLUSTER-IP       EXTERNAL-IP   PORT(S)              AGE
source-cluster-milvus                 ClusterIP   10.98.124.90     <none>        19530/TCP,9091/TCP   8m
target-cluster-milvus                 ClusterIP   10.109.234.172   <none>        19530/TCP,9091/TCP   3m
```

Prepare two types of addresses:

- Cluster addresses are written to the replication configuration and used by CDC components. These addresses must be reachable from the CDC pods.
- Client addresses are used only by your Python client when calling Milvus APIs. If you run the Python client outside the Kubernetes cluster, expose the Milvus services through your normal access method, such as a load balancer, ingress, or port-forward.

Prepare the connection information and pchannel lists for both clusters:

```python
source_cluster_addr = "http://source-cluster-milvus.milvus.svc.cluster.local:19530"
target_cluster_addr = "http://target-cluster-milvus.milvus.svc.cluster.local:19530"

source_client_addr = source_cluster_addr
target_client_addr = target_cluster_addr

# If your Python client runs outside the Kubernetes cluster, replace only
# source_client_addr and target_client_addr with externally reachable addresses.
# Keep source_cluster_addr and target_cluster_addr reachable from CDC pods.
# For example:
# source_client_addr = "http://127.0.0.1:19530"
# target_client_addr = "http://127.0.0.1:19531"

source_cluster_token = "root:Milvus"
target_cluster_token = "root:Milvus"

source_cluster_id = "source-cluster"
target_cluster_id = "target-cluster"

pchannel_num = 16
source_cluster_pchannels = [
    f"{source_cluster_id}-rootcoord-dml_{i}"
    for i in range(pchannel_num)
]
target_cluster_pchannels = [
    f"{target_cluster_id}-rootcoord-dml_{i}"
    for i in range(pchannel_num)
]
```

Replace the addresses with the actual Milvus service addresses in your environment. Do not set `source_cluster_addr` or `target_cluster_addr` to a local port-forward address unless the CDC pods can also reach that address. The pchannel list must match your Milvus deployment. Do not copy the example values without checking your cluster configuration.

## Step 5: Create the Replication Configuration

Create a replication configuration from `source-cluster` to `target-cluster`:

```python
replicate_config = {
    "clusters": [
        {
            "cluster_id": source_cluster_id,
            "connection_param": {
                "uri": source_cluster_addr,
                "token": source_cluster_token,
            },
            "pchannels": source_cluster_pchannels,
        },
        {
            "cluster_id": target_cluster_id,
            "connection_param": {
                "uri": target_cluster_addr,
                "token": target_cluster_token,
            },
            "pchannels": target_cluster_pchannels,
        },
    ],
    "cross_cluster_topology": [
        {
            "source_cluster_id": source_cluster_id,
            "target_cluster_id": target_cluster_id,
        }
    ],
}
```

## Step 6: Apply the Replication Configuration

Apply the same configuration to both clusters:

```python
from pymilvus import MilvusClient

source_client = MilvusClient(
    uri=source_client_addr,
    token=source_cluster_token,
)
target_client = MilvusClient(
    uri=target_client_addr,
    token=target_cluster_token,
)

try:
    source_client.update_replicate_configuration(**replicate_config)
    target_client.update_replicate_configuration(**replicate_config)
finally:
    source_client.close()
    target_client.close()
```

For production automation, use separate short-lived clients for this control-plane operation. This avoids sharing the same gRPC channel with application DML traffic while the cluster role is changing.

After the configuration is applied, changes written to `source-cluster` are replicated to `target-cluster`.

## Step 7: Verify Data Replication

To verify that replication works:

1. Connect to `source-cluster`.
2. Create a collection.
3. Insert data into the collection.
4. Load the collection and run a query or search on `source-cluster`.
5. Connect to `target-cluster`.
6. Run the same query or search on `target-cluster` without manually loading the collection on the standby cluster.
7. Confirm that the expected data is visible on both clusters.

The target cluster is a standby cluster in this topology. Do not run manual DDL or DCL operations, such as `load_collection`, on the standby cluster. Those operations should be performed on the source cluster and replicated to the target cluster.

The exact verification code depends on your collection schema. For a basic Milvus collection workflow, see the Milvus quick start documentation.

## CDC Lag

CDC lag is the data window between the primary and standby clusters. You should monitor it continuously after replication is configured.

CDC lag can increase when:

- The primary write rate is high.
- Network latency or packet loss increases between clusters.
- The standby cluster is overloaded.
- CDC nodes are under-provisioned.
- Large DDL or import operations are running.

Use CDC lag to guide operational decisions:

- If lag is low, planned switchover should complete faster.
- If lag is high, force failover may lose more data.

You can estimate CDC lag with the following PromQL query:

```promql
clamp_min(
  max by (channel_name) (
    milvus_wal_last_confirmed_time_tick
  )
  -
  min by (channel_name) (
    milvus_cdc_last_replicated_time_tick
  ),
  0
)
```

The result is in seconds. For each source channel, the query compares the latest confirmed WAL timetick with the last timetick replicated by CDC. If a primary replicates to multiple standby clusters, the `min by (channel_name)` expression reports the slowest replication progress for that channel.

If Prometheus scrapes multiple Milvus clusters, add label filters that match your deployment, such as `namespace` or `app_kubernetes_io_instance`, to avoid mixing metrics from different clusters.

## FAQ

### Do I need to call `update_replicate_configuration` on both clusters?

Yes. Apply the same topology to all participating clusters. If one cluster is not primary at the time of the call, it waits until the topology is applied through CDC.

### How should I choose `cluster_id`?

Use a stable, unique ID for each cluster. The ID is also used in pchannel names and replication topology references.

### Can I change pchannels after replication is configured?

You can update the topology, but the pchannel list must match the cluster layout. Treat pchannel changes as an advanced operation and verify replication carefully afterward.
