"""Generate an isolated, ephemeral distributed Milvus prototype deployment."""

import json
from pathlib import Path

namespace = "multisearch-v305"
objects = [{"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": namespace}}]


def metadata(name):
    return {"name": name, "namespace": namespace}


config = """etcd:
  endpoints: etcd:2379
  rootPath: multisearch-k8s
  auth:
    enabled: false
  use:
    embed: false
common:
  storageType: remote
  security:
    authorizationEnabled: true
minio:
  address: minio
  port: 9000
  accessKeyID: minioadmin
  secretAccessKey: minioadmin
  bucketName: multisearch-v305
  useSSL: false
mq:
  type: woodpecker
woodpecker:
  storage:
    type: minio
rootCoord:
  dmlChannelNum: 4
log:
  level: warn
quotaAndLimits:
  dql:
    enabled: true
proxy:
  multiSearch:
    maxItems: 128
    parallelism: 16
    globalParallelism: 64
    maxBatches: 8
    maxRequestBytes: 4194304
    maxResponseBytes: 16777216
    timeoutSeconds: 30
"""
objects.append(
    {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": metadata("milvus-config"),
        "data": {"user.yaml": config},
    }
)


def service(name, port, node_port=None):
    spec = {"selector": {"app": name}, "ports": [{"port": port, "targetPort": port}]}
    if node_port:
        spec["type"] = "NodePort"
        spec["ports"][0]["nodePort"] = node_port
    objects.append(
        {"apiVersion": "v1", "kind": "Service", "metadata": metadata(name), "spec": spec}
    )


def deployment(name, image, command, cpu, memory, replicas=1, extra=None, milvus=False):
    c = {
        "name": name,
        "image": image,
        "imagePullPolicy": "IfNotPresent",
        "command": command,
        "resources": {
            "requests": {"cpu": "500m", "memory": "512Mi"},
            "limits": {"cpu": str(cpu), "memory": memory},
        },
    }
    volumes = [{"name": "data", "emptyDir": {}}]
    c["volumeMounts"] = [{"name": "data", "mountPath": "/var/lib/milvus" if milvus else "/data"}]
    if milvus:
        c["imagePullPolicy"] = "Never"
        c["env"] = [
            {"name": k, "value": v}
            for k, v in {
                "ETCD_USE_EMBED": "false",
                "COMMON_STORAGETYPE": "remote",
                "DEPLOY_MODE": "DISTRIBUTED",
                "ETCD_ENDPOINTS": "etcd:2379",
                "MINIO_ADDRESS": "minio",
                "MQ_TYPE": "woodpecker",
            }.items()
        ]
        c["volumeMounts"].append(
            {"name": "config", "mountPath": "/milvus/configs/user.yaml", "subPath": "user.yaml"}
        )
        volumes.append({"name": "config", "configMap": {"name": "milvus-config"}})
        c["readinessProbe"] = {
            "httpGet": {"path": "/healthz", "port": 9091},
            "initialDelaySeconds": 10,
            "periodSeconds": 5,
            "timeoutSeconds": 3,
        }
    if extra:
        c.update(extra)
    objects.append(
        {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": metadata(name),
            "spec": {
                "replicas": replicas,
                "strategy": {"type": "Recreate"},
                "selector": {"matchLabels": {"app": name}},
                "template": {
                    "metadata": {"labels": {"app": name}},
                    "spec": {
                        "enableServiceLinks": False,
                        "securityContext": {"fsGroup": 1000},
                        "containers": [c],
                        "volumes": volumes,
                        "terminationGracePeriodSeconds": 30,
                    },
                },
            },
        }
    )


deployment(
    "etcd",
    "quay.io/coreos/etcd:v3.5.25",
    [
        "etcd",
        "--data-dir=/data/etcd",
        "--advertise-client-urls=http://etcd:2379",
        "--listen-client-urls=http://0.0.0.0:2379",
        "--quota-backend-bytes=4294967296",
    ],
    1,
    "1Gi",
)
service("etcd", 2379)
deployment(
    "minio",
    "minio/minio:RELEASE.2024-05-28T17-19-04Z",
    ["minio", "server", "/data"],
    1,
    "1Gi",
    extra={
        "env": [
            {"name": "MINIO_ROOT_USER", "value": "minioadmin"},
            {"name": "MINIO_ROOT_PASSWORD", "value": "minioadmin"},
        ]
    },
)
service("minio", 9000)
for name, cpu, memory, replicas in [
    ("mixcoord", 2, "3Gi", 1),
    ("proxy", 2, "2Gi", 2),
    ("querynode", 4, "4Gi", 1),
    ("datanode", 2, "4Gi", 1),
    ("streamingnode", 4, "6Gi", 1),
]:
    deployment(
        name, "milvus-multisearch:v305", ["milvus", "run", name], cpu, memory, replicas, milvus=True
    )
service("proxy", 19530, 30531)
Path(__file__).with_name("kubernetes.json").write_text(
    json.dumps({"apiVersion": "v1", "kind": "List", "items": objects}, indent=2) + "\n"
)
