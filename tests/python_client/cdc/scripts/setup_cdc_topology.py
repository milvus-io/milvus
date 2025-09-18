from pymilvus import MilvusClient
from concurrent.futures import ThreadPoolExecutor, as_completed

def setup_cdc_topology(upstream_uri, downstream_uri, removed_clusters_uri, upstream_token, downstream_token, removed_clusters_token, source_cluster_id, target_cluster_id, removed_clusters_id, pchannel_num):
    print(f"DEBUG: upstream_uri: {upstream_uri}, downstream_uri: {downstream_uri}, upstream_token: {upstream_token}, downstream_token: {downstream_token}, source_cluster_id: {source_cluster_id}, target_cluster_id: {target_cluster_id}, pchannel_num: {pchannel_num}")
    upstream_client = MilvusClient(uri=upstream_uri, token=upstream_token)

    # Parse comma-separated lists
    if isinstance(downstream_uri, str) and ',' in downstream_uri:
        downstream_uris = [uri.strip() for uri in downstream_uri.split(',')]
    else:
        downstream_uris = [downstream_uri] if isinstance(downstream_uri, str) else downstream_uri

    if isinstance(target_cluster_id, str) and ',' in target_cluster_id:
        target_cluster_ids = [cluster_id.strip() for cluster_id in target_cluster_id.split(',')]
    else:
        target_cluster_ids = [target_cluster_id] if isinstance(target_cluster_id, str) else target_cluster_id

    if isinstance(removed_clusters_uri, str) and ',' in removed_clusters_uri:
        removed_clusters_uris = [uri.strip() for uri in removed_clusters_uri.split(',')]
    else:
        removed_clusters_uris = [removed_clusters_uri] if isinstance(removed_clusters_uri, str) else removed_clusters_uri

    if isinstance(removed_clusters_id, str) and ',' in removed_clusters_id:
        removed_clusters_ids = [cluster_id.strip() for cluster_id in removed_clusters_id.split(',')]
    else:
        removed_clusters_ids = [removed_clusters_id] if isinstance(removed_clusters_id, str) else removed_clusters_id

    # Ensure we have matching numbers of downstream URIs and cluster IDs
    if len(downstream_uris) != len(target_cluster_ids):
        raise ValueError(f"Number of downstream URIs ({len(downstream_uris)}) must match number of target cluster IDs ({len(target_cluster_ids)})")

    # Create downstream clients
    downstream_clients = []
    for downstream_uri_single in downstream_uris:
        print(f"DEBUG: downstream_uri_single: {downstream_uri_single}, downstream_token: {downstream_token}")
        downstream_clients.append(MilvusClient(uri=downstream_uri_single, token=downstream_token))

    # Build clusters configuration
    clusters = [
        {
            "cluster_id": source_cluster_id,
            "connection_param": {
                "uri": upstream_uri,
                "token": upstream_token
            },
            "pchannels": [f"{source_cluster_id}-rootcoord-dml_{i}" for i in range(pchannel_num)]
        }
    ]

    # Add all target clusters
    for target_id, target_uri in zip(target_cluster_ids, downstream_uris):
        clusters.append({
            "cluster_id": target_id,
            "connection_param": {
                "uri": target_uri,
                "token": downstream_token
            },
            "pchannels": [f"{target_id}-rootcoord-dml_{j}" for j in range(pchannel_num)]
        })

    # Build cross-cluster topology
    cross_cluster_topology = []
    for target_id in target_cluster_ids:
        cross_cluster_topology.append({
            "source_cluster_id": source_cluster_id,
            "target_cluster_id": target_id
        })

    config = {
        "clusters": clusters,
        "cross_cluster_topology": cross_cluster_topology
    }

    # Update configuration on all clients using multi-threading
    print(f"DEBUG: config: {config}")

    def update_client_config(client, config_to_use, client_type=""):
        try:
            client.update_replicate_configuration(**config_to_use)
            return f"{client_type} updated successfully"
        except Exception as e:
            print(f"Failed to update {client_type}: {e}")
            raise e

    # Collect all update tasks
    update_tasks = []

    # Add upstream and downstream clients with normal config
    all_clients = [upstream_client] + downstream_clients
    for client in all_clients:
        update_tasks.append((client, config, "Normal client"))

    # Handle removed clusters - prepare them with empty topology
    if removed_clusters_uris and removed_clusters_uris[0]:  # Check if removed_clusters_uris is not empty
        for removed_uri, removed_id in zip(removed_clusters_uris, removed_clusters_ids):
            if removed_uri and removed_id:  # Skip empty URIs and IDs
                print(f"DEBUG: removed_cluster_uri: {removed_uri}, removed_clusters_token: {removed_clusters_token}")
                removed_client = MilvusClient(uri=removed_uri, token=removed_clusters_token)

                empty_config = {
                    "clusters": [
                        {
                            "cluster_id": removed_id,
                            "connection_param": {
                                "uri": removed_uri,
                                "token": removed_clusters_token
                            },
                            "pchannels": [f"{removed_id}-rootcoord-dml_{i}" for i in range(pchannel_num)]
                        }
                    ],
                    "cross_cluster_topology": []
                }
                print(f"DEBUG: Removing cluster {removed_id} with empty config: {empty_config}")
                update_tasks.append((removed_client, empty_config, f"Removed cluster {removed_id}"))

    # Use single ThreadPoolExecutor to update all clients concurrently
    with ThreadPoolExecutor(max_workers=len(update_tasks)) as executor:
        # Submit all update tasks
        futures = [executor.submit(update_client_config, client, config_to_use, client_type)
                   for client, config_to_use, client_type in update_tasks]

        # Wait for all tasks to complete
        for future in as_completed(futures):
            try:
                result = future.result()
                print(f"Task completed: {result}")
            except Exception as e:
                print(f"Task failed with error: {e}")
                raise e


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='connection info')
    parser.add_argument('--upstream_uri', type=str, default='10.100.36.179', help='milvus host')
    parser.add_argument('--downstream_uri', type=str, default='10.100.36.178', help='milvus host')
    parser.add_argument('--removed_clusters_uri', type=str, default='', help='milvus host')
    parser.add_argument('--upstream_token', type=str, default='root:Milvus', help='milvus token')
    parser.add_argument('--downstream_token', type=str, default='root:Milvus', help='milvus token')
    parser.add_argument('--removed_clusters_token', type=str, default='root:Milvus', help='milvus token')
    parser.add_argument('--source_cluster_id', type=str, default='cdc-test-source', help='source cluster id')
    parser.add_argument('--target_cluster_id', type=str, default='cdc-test-target', help='target cluster id')
    parser.add_argument('--removed_clusters_id', type=str, default='', help='removed clusters id')

    parser.add_argument('--pchannel_num', type=int, default=16, help='pchannel num')
    args = parser.parse_args()
    setup_cdc_topology(args.upstream_uri, 
                        args.downstream_uri, 
                        args.removed_clusters_uri,
                        args.upstream_token, 
                        args.downstream_token, 
                        args.removed_clusters_token,
                        args.source_cluster_id, 
                        args.target_cluster_id,
                        args.removed_clusters_id,
                        args.pchannel_num)