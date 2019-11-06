This document is a gentle introduction to Milvus Cluster, that does not use complex to understand distributed systems concepts. It provides instructions about how to setup a cluster, test, and operate it, without going into the details that are covered in the Milvus Cluster specification but just describing how the system behaves from the point of view of the user.

However this tutorial tries to provide information about the availability and consistency characteristics of Milvus Cluster from the point of view of the final user, stated in a simple to understand way.

If you plan to run a serious Milvus Cluster deployment, the more formal specification is a suggested reading, even if not strictly required. However it is a good idea to start from this document, play with Milvus Cluster some time, and only later read the specification.

## Milvus Cluster Introduction
### Infrastructure
* Kubenetes Cluster With Nvida GPU Node
* Install Nvida Docker in Cluster

### Requried Docker Registry
* Milvus Server:      ```registry.zilliz.com/milvus/engine:${version>=0.3.1}```
* Milvus Celery Apps: ```registry.zilliz.com/milvus/celery-apps:${version>=v0.2.1}```

### Cluster Ability
* Milvus Cluster provides a way to run a Milvus installation where query requests are automatically sharded across multiple milvus readonly nodes.
* Milvus Cluster provides availability during partitions, that is in pratical terms the ability to continue the operations when some nodes fail or are not able to communicate.

### Metastore
Milvus supports 2 backend databases for deployment:
* Splite3: Single mode only.
* MySQL: Single/Cluster mode
* ETCD: `TODO`

### Storage
Milvus supports 2 backend storage for deployment:
* Local filesystem: Convenient for use and deployment but not reliable.
* S3 OOS: Reliable: Need extra configuration. Need external storage service.

### Message Queue
Milvus supports various MQ backend for deployment:
* Redis
* Rabbitmq
* MySQL/PG/MongoDB

### Cache
* Milvus supports `Redis` as Cache backend for deployment. To reduce the system complexity, we recommend to use `Redis` as MQ backend.

### Workflow
* Milvus Cluster use Celery as workflow scheduler.
* Milvus Cluster workflow calculation node can be scaled.
* Milvus Cluster only contains 1 worflow monitor node. Monitor node detects caculation nodes status and provides decision for work scheduling.
* Milvus Cluster supports different workflow result backend and we recommend to use `Redis` as result backend for performance consideration.

### Writeonly Node
* Milvus can be configured in write-only mode.
* Right now Milvus Cluster only provide 1 write-only node.

### Readonly Node
* Milvus can be configured in readonly mode.
* Milvus Cluster automatically shard incoming query requests across multiple readonly nodes.
* Milvus Cluster supports readonly nodes scaling.
* Milvus Cluster provides pratical solution to avoid performance degradation during cluster rebalance.

### Proxy
* Milvus Cluster communicates with clients by proxy.
* Milvus Cluster supports proxy scaling.

### Monitor
* Milvus Cluster suports metrics monitoring by prometheus.
* Milvus Cluster suports workflow tasks monitoring by flower.
* Milvus Cluster suports cluster monitoring by all kubernetes ecosystem monitoring tools.

## Milvus Cluster Kubernetes Resources
### PersistentVolumeClaim
* LOG PersistentVolume: `milvus-log-disk`

### ConfigMap
* Celery workflow configmap: `milvus-celery-configmap`::`milvus_celery_config.yml`
* Proxy configmap: `milvus-proxy-configmap`::`milvus_proxy_config.yml`
* Readonly nodes configmap: `milvus-roserver-configmap`::`config.yml`, `milvus-roserver-configmap`::`log.conf`
* Write-only nodes configmap: `milvus-woserver-configmap`::`config.yml`, `milvus-woserver-configmap`::`log.conf`

### Services
* Mysql service: `milvus-mysql`
* Redis service: `milvus-redis`
* Rroxy service: `milvus-proxy-servers`
* Write-only servers service: `milvus-wo-servers`

### StatefulSet
* Readonly stateful servers: `milvus-ro-servers`

### Deployment
* Worflow monitor: `milvus-monitor`
* Worflow workers: `milvus-workers`
* Write-only servers: `milvus-wo-servers`
* Proxy: `milvus-proxy`

## Milvus Cluster Configuration
### Write-only server:
```milvus-woserver-configmap::config.yml:
  server_config.mode: cluster
  db_config.db_backend_url: mysql://${user}:${password}@milvus-mysql/${dbname}
```
### Readonly server:
```milvus-roserver-configmap::config.yml:
  server_config.mode: read_only
  db_config.db_backend_url: mysql://\${user}:${password}@milvus-mysql/${dbname}
```
### Celery workflow:
```milvus-celery-configmap::milvus_celery_config.yml:
  DB_URI=mysql+mysqlconnector://${user}:${password}@milvus-mysql/${dbname}
```
### Proxy workflow:
```milvus-proxy-configmap::milvus_proxy_config.yml:
```
