# Per-Cluster mTLS for CDC Cross-Cluster Replication

## Overview

Starting from v2.6.x, Milvus CDC supports **per-cluster mTLS configuration** for cross-cluster replication. Each target cluster can have its own CA certificate, client certificate, and client key, enabling secure replication across clusters that use independent certificate authorities.

Key capabilities:

- **Independent CA per cluster**: Each cluster can have its own Certificate Authority, preventing cross-cluster credential misuse
- **Per-cluster client identity**: CDC uses a distinct client certificate for each target cluster, enabling fine-grained access control and audit

## Quick Start

A minimal 2-cluster example: cluster A (by-dev1) replicates to cluster B (by-dev2), each with its own CA.

**1. Generate per-cluster certs**

```bash
for i in 1 2; do
    openssl genrsa -out ca-dev${i}.key 4096 2>/dev/null
    openssl req -new -x509 -key ca-dev${i}.key -out ca-dev${i}.pem -days 3650 -subj "/CN=CA-dev${i}"

    openssl genrsa -out server-dev${i}.key 2048 2>/dev/null
    openssl req -new -key server-dev${i}.key -out server-dev${i}.csr -subj "/CN=server-dev${i}"
    openssl x509 -req -in server-dev${i}.csr -CA ca-dev${i}.pem -CAkey ca-dev${i}.key \
        -CAcreateserial -out server-dev${i}.pem -days 3650 \
        -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1")

    openssl genrsa -out client-dev${i}.key 2048 2>/dev/null
    openssl req -new -key client-dev${i}.key -out client-dev${i}.csr -subj "/CN=cdc-to-dev${i}"
    openssl x509 -req -in client-dev${i}.csr -CA ca-dev${i}.pem -CAkey ca-dev${i}.key \
        -CAcreateserial -out client-dev${i}.pem -days 3650
done
rm -f *.csr *.srl
```

**2. Add per-cluster TLS to each cluster's `milvus.yaml`**

Each cluster uses its own server cert and CA. The `tls.clusters` section (for CDC outbound) is the same on all clusters.

Cluster A (`milvus.yaml`):

```yaml
tls:
  serverPemPath: /certs/server-dev1.pem
  serverKeyPath: /certs/server-dev1.key
  caPemPath: /certs/ca-dev1.pem
  clusters:
    by-dev1:
      caPemPath: /certs/ca-dev1.pem
      clientPemPath: /certs/client-dev1.pem
      clientKeyPath: /certs/client-dev1.key
    by-dev2:
      caPemPath: /certs/ca-dev2.pem
      clientPemPath: /certs/client-dev2.pem
      clientKeyPath: /certs/client-dev2.key
```

Cluster B (`milvus.yaml`):

```yaml
tls:
  serverPemPath: /certs/server-dev2.pem
  serverKeyPath: /certs/server-dev2.key
  caPemPath: /certs/ca-dev2.pem
  clusters:
    by-dev1:
      caPemPath: /certs/ca-dev1.pem
      clientPemPath: /certs/client-dev1.pem
      clientKeyPath: /certs/client-dev1.key
    by-dev2:
      caPemPath: /certs/ca-dev2.pem
      clientPemPath: /certs/client-dev2.pem
      clientKeyPath: /certs/client-dev2.key
```

**3. Start clusters with mTLS**

```bash
# Each cluster uses its own server cert
export COMMON_SECURITY_TLSMODE=2

# Cluster A
TLS_CAPEMPATH=/certs/ca-dev1.pem TLS_SERVERPEMPATH=/certs/server-dev1.pem \
    TLS_SERVERKEYPATH=/certs/server-dev1.key ./milvus run standalone

# Cluster B (separate machine or different ports)
TLS_CAPEMPATH=/certs/ca-dev2.pem TLS_SERVERPEMPATH=/certs/server-dev2.pem \
    TLS_SERVERKEYPATH=/certs/server-dev2.key ./milvus run standalone
```

**4. Configure replication (A -> B)**

```python
from pymilvus import MilvusClient

client = MilvusClient(
    uri="https://localhost:19530", token="root:Milvus",
    ca_pem_path="/certs/ca-dev1.pem",
    client_pem_path="/certs/client-dev1.pem",
    client_key_path="/certs/client-dev1.key",
)

client.update_replicate_configuration(
    clusters=[
        {"cluster_id": "by-dev1",
         "connection_param": {"uri": "https://cluster-a:19530", "token": "root:Milvus"},
         "pchannels": [f"by-dev1-rootcoord-dml_{i}" for i in range(16)]},
        {"cluster_id": "by-dev2",
         "connection_param": {"uri": "https://cluster-b:19531", "token": "root:Milvus"},
         "pchannels": [f"by-dev2-rootcoord-dml_{i}" for i in range(16)]},
    ],
    cross_cluster_topology=[
        {"source_cluster_id": "by-dev1", "target_cluster_id": "by-dev2"},
    ],
)
```

CDC will now use `ca-dev2.pem` + `client-dev2.pem` when connecting to cluster B, and log `"CDC outbound TLS enabled" [targetCluster=by-dev2]`.

## Background

Milvus CDC replicates data between clusters by consuming change streams from a source cluster and writing to one or more target clusters. When clusters enforce mTLS (`tlsMode=2`), CDC must present a valid client certificate trusted by each target cluster's CA.

Previous versions required a **shared CA** across all clusters, meaning a single `caPemPath` was used for all outbound connections. This has two drawbacks:

1. **No isolation**: If CDC accidentally uses the wrong client cert for a target, the shared CA still trusts it, masking configuration errors
2. **Operational risk**: Compromising the shared CA affects all clusters simultaneously

Per-cluster mTLS solves both problems by allowing each cluster to operate under its own CA.

## Configuration

### Parameters

Per-cluster TLS is configured under the `tls.clusters` section in `milvus.yaml`. Each entry is keyed by the cluster ID (the value used in `UpdateReplicateConfiguration`).

| Parameter | Type | Description |
|---|---|---|
| `tls.clusters.<clusterID>.caPemPath` | string | Path to the CA certificate used to verify the target cluster's server certificate |
| `tls.clusters.<clusterID>.clientPemPath` | string | Path to the client certificate presented to the target cluster |
| `tls.clusters.<clusterID>.clientKeyPath` | string | Path to the client private key |

**Activation rule**: TLS is enabled for a target cluster only when **both** `clientPemPath` and `clientKeyPath` are set. If only `caPemPath` is set, TLS is not activated. TLS 1.3 is enforced as the minimum version.

### Server-Side TLS Parameters

Each cluster's Milvus server also needs its own TLS configuration:

| Parameter | Type | Description |
|---|---|---|
| `common.security.tlsMode` | int | `0` = disabled, `1` = one-way TLS, `2` = mTLS |
| `tls.caPemPath` | string | CA certificate for verifying client certs (server-side) |
| `tls.serverPemPath` | string | Server certificate (must have SAN matching the server's hostname/IP) |
| `tls.serverKeyPath` | string | Server private key |

### Configuration Example

A 3-cluster deployment where each cluster has its own CA:

```yaml
# milvus.yaml — CDC outbound TLS configuration
tls:
  serverPemPath: /certs/server-dev1.pem
  serverKeyPath: /certs/server-dev1.key
  caPemPath: /certs/ca-dev1.pem
  clusters:
    by-dev1:
      caPemPath: /certs/ca-dev1.pem
      clientPemPath: /certs/client-dev1.pem
      clientKeyPath: /certs/client-dev1.key
    by-dev2:
      caPemPath: /certs/ca-dev2.pem
      clientPemPath: /certs/client-dev2.pem
      clientKeyPath: /certs/client-dev2.key
    by-dev3:
      caPemPath: /certs/ca-dev3.pem
      clientPemPath: /certs/client-dev3.pem
      clientKeyPath: /certs/client-dev3.key
```

> **Note**: The top-level `tls.serverPemPath`, `tls.serverKeyPath`, and `tls.caPemPath` configure the cluster's own server-side TLS. The `tls.clusters.*` section configures CDC's **outbound** client credentials per target cluster.

## Certificate Generation

### Per-Cluster CA and Certificates

Generate an independent CA and certificates for each cluster. Below is an example for 3 clusters:

```bash
#!/bin/bash
CERT_DIR="./certs"
DAYS=3650
mkdir -p "${CERT_DIR}"

for i in 1 2 3; do
    # CA (self-signed)
    openssl genrsa -out "${CERT_DIR}/ca-dev${i}.key" 4096
    openssl req -new -x509 -key "${CERT_DIR}/ca-dev${i}.key" \
        -out "${CERT_DIR}/ca-dev${i}.pem" -days ${DAYS} \
        -subj "/CN=MilvusCA-dev${i}"

    # Server cert (SAN must match how clients connect)
    openssl genrsa -out "${CERT_DIR}/server-dev${i}.key" 2048
    openssl req -new -key "${CERT_DIR}/server-dev${i}.key" \
        -out "${CERT_DIR}/server-dev${i}.csr" -subj "/CN=milvus-server-dev${i}"
    openssl x509 -req -in "${CERT_DIR}/server-dev${i}.csr" \
        -CA "${CERT_DIR}/ca-dev${i}.pem" -CAkey "${CERT_DIR}/ca-dev${i}.key" \
        -CAcreateserial -out "${CERT_DIR}/server-dev${i}.pem" -days ${DAYS} \
        -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1")

    # CDC client cert (for CDC connecting TO this cluster)
    openssl genrsa -out "${CERT_DIR}/client-dev${i}.key" 2048
    openssl req -new -key "${CERT_DIR}/client-dev${i}.key" \
        -out "${CERT_DIR}/client-dev${i}.csr" -subj "/CN=cdc-to-dev${i}"
    openssl x509 -req -in "${CERT_DIR}/client-dev${i}.csr" \
        -CA "${CERT_DIR}/ca-dev${i}.pem" -CAkey "${CERT_DIR}/ca-dev${i}.key" \
        -CAcreateserial -out "${CERT_DIR}/client-dev${i}.pem" -days ${DAYS}
done

rm -f "${CERT_DIR}"/*.csr "${CERT_DIR}"/*.srl
```

### What Gets Generated

For each cluster `dev{i}`:

| File | Signed By | Purpose |
|---|---|---|
| `ca-dev{i}.pem` / `ca-dev{i}.key` | Self-signed | Certificate Authority for cluster by-dev{i} |
| `server-dev{i}.pem` / `server-dev{i}.key` | `ca-dev{i}` | Server certificate (used by Milvus proxy/nodes) |
| `client-dev{i}.pem` / `client-dev{i}.key` | `ca-dev{i}` | CDC client certificate for connecting TO by-dev{i} |

### Why Separate CAs Matter

With a shared CA, using the wrong `caPemPath` for a target cluster silently succeeds — the shared CA trusts all certificates. With per-cluster CAs:

- `ca-dev1.pem` only trusts `server-dev1.pem` and `client-dev1.pem`
- Connecting to cluster B (`server-dev2.pem`) with cluster A's CA (`ca-dev1.pem`) fails with `CERTIFICATE_VERIFY_FAILED`
- This ensures CDC configuration errors are caught immediately rather than silently allowing misrouted connections

## Setting Up Replication

### Step 1: Configure mTLS on Each Cluster

Each cluster's server-side TLS is configured via environment variables or `milvus.yaml`:

```bash
# Cluster A (by-dev1)
export COMMON_SECURITY_TLSMODE=2
export TLS_CAPEMPATH=/certs/ca-dev1.pem
export TLS_SERVERPEMPATH=/certs/server-dev1.pem
export TLS_SERVERKEYPATH=/certs/server-dev1.key
```

### Step 2: Configure Per-Cluster CDC Outbound TLS

Add the `tls.clusters` section to `milvus.yaml` as shown in [Configuration Example](#configuration-example). Since cluster IDs contain dashes (e.g., `by-dev1`), these **must** be configured in `milvus.yaml` — environment variables cannot represent dashes in nested key names.

Point `MILVUSCONF` to the directory containing the modified `milvus.yaml`:

```bash
export MILVUSCONF=/path/to/config-dir
```

### Step 3: Configure Replication Topology

Use the `UpdateReplicateConfiguration` API via PyMilvus. The API only carries `uri` and `token` — no TLS fields:

```python
from pymilvus import MilvusClient

# Connect to each cluster with its own CA and client cert
client_a = MilvusClient(
    uri="https://cluster-a:19530", token="root:Milvus",
    ca_pem_path="/certs/ca-dev1.pem",
    client_pem_path="/certs/pymilvus-dev1.pem",
    client_key_path="/certs/pymilvus-dev1.key",
)

# Build replication config: A -> B, A -> C
config = {
    "clusters": [
        {
            "cluster_id": "by-dev1",
            "connection_param": {"uri": "https://cluster-a:19530", "token": "root:Milvus"},
            "pchannels": [f"by-dev1-rootcoord-dml_{i}" for i in range(16)],
        },
        {
            "cluster_id": "by-dev2",
            "connection_param": {"uri": "https://cluster-b:19531", "token": "root:Milvus"},
            "pchannels": [f"by-dev2-rootcoord-dml_{i}" for i in range(16)],
        },
        {
            "cluster_id": "by-dev3",
            "connection_param": {"uri": "https://cluster-c:19532", "token": "root:Milvus"},
            "pchannels": [f"by-dev3-rootcoord-dml_{i}" for i in range(16)],
        },
    ],
    "cross_cluster_topology": [
        {"source_cluster_id": "by-dev1", "target_cluster_id": "by-dev2"},
        {"source_cluster_id": "by-dev1", "target_cluster_id": "by-dev3"},
    ],
}

# Apply to ALL clusters (the current primary processes the config change)
client_a.update_replicate_configuration(**config)
```

> **Important**: Send `UpdateReplicateConfiguration` to **all clusters** in parallel. Only the current primary processes it, but if you don't know which cluster is primary (e.g., after a switchover), sending to all ensures the config is applied.

## Troubleshooting

### Verify Per-Cluster Cert Selection in CDC Logs

When CDC establishes outbound connections, it logs the cert paths for each target:

```
[INFO] [cluster/milvus_client.go:78] ["CDC outbound TLS enabled"]
    [targetCluster=by-dev2]
    [caPemPath=/certs/ca-dev2.pem]
    [clientPemPath=/certs/client-dev2.pem]
    [clientKeyPath=/certs/client-dev2.key]
```

Verify that:
- Each target cluster has a **different** `caPemPath` (e.g., `ca-dev2.pem` for by-dev2, not `ca.pem`)
- Each target cluster has the **matching** `clientPemPath` (e.g., `client-dev2.pem` for by-dev2)

### Common Errors

| Symptom | Cause | Fix |
|---|---|---|
| `CERTIFICATE_VERIFY_FAILED` in CDC logs | Wrong `caPemPath` for target cluster | Ensure `tls.clusters.<id>.caPemPath` matches the CA that signed the target's server cert |
| `TLSV1_ALERT_UNKNOWN_CA` in CDC logs | Target cluster doesn't trust CDC's client cert | Ensure the client cert is signed by the CA configured as the target's server-side `tls.caPemPath` |
| `TLSV1_ALERT_CERTIFICATE_REQUIRED` | CDC connecting without client cert | Ensure both `clientPemPath` and `clientKeyPath` are set in `tls.clusters.<id>` |
| CDC log shows no "CDC outbound TLS enabled" | Missing or incomplete config | Verify `tls.clusters` section exists in the `milvus.yaml` at `MILVUSCONF` path, not the source checkout |
| `UpdateReplicateConfiguration` hangs | Sent to a secondary cluster only | Send to all clusters in parallel so the actual primary processes it |

### Verify Cross-CA Isolation

To confirm that per-cluster CAs are actually enforced, connect to one cluster using another cluster's CA — it should fail:

```bash
# This should FAIL — ca-dev1 does not trust server-dev2
curl --cacert /certs/ca-dev1.pem \
     --cert /certs/client-dev1.pem \
     --key /certs/client-dev1.key \
     https://cluster-b:19531/healthz
# Expected: SSL certificate problem: certificate verify failed
```

## Switchover

When switching the primary (e.g., from A to B), simply call `UpdateReplicateConfiguration` with the new topology on **all clusters**:

```python
# Switchover: B becomes primary, replicates to A and C
config["cross_cluster_topology"] = [
    {"source_cluster_id": "by-dev2", "target_cluster_id": "by-dev1"},
    {"source_cluster_id": "by-dev2", "target_cluster_id": "by-dev3"},
]

# Send to all clusters in parallel
from concurrent.futures import ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=3) as executor:
    for client in [client_a, client_b, client_c]:
        executor.submit(client.update_replicate_configuration, **config)
```

No certificate changes are needed — each cluster's CDC already has the per-cluster TLS config for all possible targets.

## Version Compatibility

| Feature | Minimum Version |
|---|---|
| CDC cross-cluster replication | v2.6.x |
| Per-cluster mTLS (`tls.clusters.*`) | v2.6.x |
| `UpdateReplicateConfiguration` API | v2.6.x |
| PyMilvus `update_replicate_configuration` | v2.6.x |
