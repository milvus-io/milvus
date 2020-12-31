# A solution to achive low latency and high throughput
We can get from [Mishards](../shards/README.md) milvus can be deployed with Mishards  as cluster to support massive-scale(10 billion, 100 billion or even larger datasets). However, when it comes to case that datasets is limited(say, millions or tens million) and low latency、high throughput is required, Mishards cannot handle it.
Here, we give a proposal to solve the problem above in production environment based on envoy.

## How to
> Under kubernetes, we can use [kubectl](https://kubernetes.io/docs/reference/generated/kubectl/kubectl-commands#apply) to handle resources

### Step 1: Docker and Kubernetes environment
If no Docker and Kubernetes environment available, you can build one with [docker](https://v1-17.docs.kubernetes.io/docs/setup/production-environment/container-runtimes/#docker) and [Highly Available Kubernetes clusters](https://v1-17.docs.kubernetes.io/docs/setup/production-environment/tools/kubeadm/high-availability/) as reference.

### Step 2: Distributed file system
Milvus cluster relys on [Distributed file system](https://en.wikipedia.org/wiki/Clustered_file_system) to hold data. We can know from [Storge Classes](https://kubernetes.io/docs/concepts/storage/storage-classes/), kubernetes support plenty of plugins to communicate with specific storage engine(such as CephFs、AWS EBS、Azure Disk、GCE PD、Glusterfs ...)

If one Distributed file system is already available, this step can be skipped, otherwise, [GlusterFs](https://www.gluster.org/) can be a option, and we can deploy it following [gluster-kubernetes
](https://github.com/gluster/gluster-kubernetes/blob/master/docs/setup-guide.md)

After we deployed glusterfs, we can create [StorageClass](https://kubernetes.io/docs/concepts/storage/storage-classes/) following [glusterfs StorageClass](https://kubernetes.io/docs/concepts/storage/storage-classes/#glusterfs). To be brief, we can create StorageClass from [storageclass.yaml](storage/storageclass.yaml)
```sh
kubectl apply -f storage/storageclass.yaml
```

### Step 3: Namespace
We can create a [Namespace](https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/) for isolation, and all the resource created will be within this namespace(if we need more Milvus cluster, we can create more namespace).
```sh
kubectl apply -f kubernetes/milvus_namespace.yaml
```
What is important to keep in mind is that `StorageClass` created in step 2 can be used across namespaces.

### Step 4: PVC
Based on StorageClass from step 2, we can create [PVC](https://kubernetes.io/docs/concepts/storage/persistent-volumes/) from [milvus_pvc.yaml](kubernetes/milvus_pvc.yaml). Pvcs will be mounted to mysql、envoy、milvus in the following steps.

```sh
kubectl apply -f kubernetes/milvus_pvc.yaml
```
### Step 5. Configmap
[Configmap](https://kubernetes.io/docs/concepts/configuration/configmap/) can be used to store all the configurations to be used in the following steps.
```sh
kubectl apply -f kubernetes/configmap/*
```
### Step 6: Mysql
Milvus use mysql to support cluster deployment.
From [Deploying MySQL Server with Docker](https://dev.mysql.com/doc/refman/5.7/en/docker-mysql-more-topics.html) we known, mysql server can deploy with docker and we can create mysql service from [milvus_mysql.yaml](kubernetes/milvus_mysql.yaml).
```sh
kubectl apply -f kubernetes/milvus_mysql.yaml
```
### Step 7: Milvus rw/ro
Following [milvus_rw_servers.yaml](kubernetes/milvus_rw_servers.yaml) and [milvus_ro_servers.yaml](kubernetes/milvus_ro_servers.yaml), we can create milvus rw、ro services.
### Step 8: envoy
**The last but the most.**

[Envoy](https://www.envoyproxy.io/) is an open source edge and service proxy, designed for cloud-native application, we can use envoy to distinguish milvus read/write and get scalability when high throughput is required.
We can create envoy following [envoy_proxy.yaml](kubernetes/envoy_proxy.yaml)

```sh
kubectl apply -f kubernetes/envoy_proxy.yaml
```

Milvus read/write can be distinguished following configuration piece in [envoy-configmap.yaml](kubernetes/configmap/envoy-configmap.yaml) as below:
```yaml
... ...
                  domains:
                  - "*"
                  routes:
                  - match:
                      prefix: "/milvus.grpc.MilvusService/Search"
                    route:
                      cluster: milvus_backend_ro
                      timeout: 1s
                      priority: HIGH
                  - match:
                      prefix: "/"
                    route:
                      cluster: milvus_backend_rw
                      timeout: 3600s
                      priority: HIGH
... ...  
```

Milvus can get get scalability following configuration piece in [envoy-configmap.yaml](kubernetes/configmap/envoy-configmap.yaml) and [milvus_ro_servers.yaml](kubernetes/milvus_ro_servers.yaml) as below([Headless Service](https://kubernetes.io/docs/concepts/services-networking/service/#headless-services) and [Strict DNS](https://www.envoyproxy.io/docs/envoy/v1.11.0/intro/arch_overview/upstream/service_discovery#strict-dns) can help to get scalability when [gRPC](https://grpc.io/blog/grpc-on-http2/#resolvers-and-load-balancers) is used):

`envoy-configmap.yaml`
```yaml
... ...
      clusters:
      - name: milvus_backend_ro
        type: STRICT_DNS
        connect_timeout: 1s
        lb_policy: ROUND_ROBIN
        dns_lookup_family: V4_ONLY
        http2_protocol_options: {}
        circuit_breakers:
          thresholds:
            priority: HIGH
            max_pending_requests: 20480
            max_connections: 20480
            max_requests: 20480
            max_retries: 1
        load_assignment:
          cluster_name: milvus_backend_ro
          endpoints:
          - lb_endpoints:
            - endpoint:
                address:
                  socket_address:
                    address: milvus-ro-servers
                    port_value: 19530
                    protocol: TCP
... ...  
```

`milvus_ro_servers.yaml`
```yaml
... ...
kind: Service
apiVersion: v1
metadata:
  name: milvus-ro-servers
  namespace: milvus-demo
spec:
  type: ClusterIP
  clusterIP: None
  selector:
    app: milvus
    tier: ro-servers
  ports:
    - protocol: TCP
      port: 19530
      targetPort: 19530
      name: engine
    - protocol: TCP
      port: 19121
      targetPort: 19121
      name: web
... ...  
```

Finally, we can access milvus grpc api at port `32000` as configuration piece in [envoy_proxy.yaml](kubernetes/envoy_proxy.yaml) below:
```yaml
apiVersion: v1
kind: Service
metadata:
  name: envoy
  namespace: milvus-demo
spec:
  type: NodePort
  ports:
  - port: 80
    targetPort: 80
    nodePort: 32000
  selector:
    app: milvus
    tier: proxy
```
## Extra
### Speed up image pulling
To speed up image pulling, a [private registry](https://goharbor.io/) can be deployed, and we can add extra configuration([specifying-imagepullsecrets-on-a-pod](https://kubernetes.io/docs/concepts/containers/images/#specifying-imagepullsecrets-on-a-pod)) to use it. 

### Resource assign
[Managing Resources for Containers](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/) can tells us how memory and cpu usage can be controlled.

### Pod schedule
As described in [assign-pod-node](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node), we can use [NodeSelector](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#nodeselector) and [Affinity](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#affinity-and-anti-affinity) to control where Milvus pod can be located

### Probes
[Probes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/) can be used to detect when Milvus pod is ready, when Milvus pod need to be restarted.

### DaemonSet vs Deployment
[DaemonSet](https://kubernetes.io/docs/concepts/workloads/controllers/daemonset/) and [Deployment](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/) are two optional ways to deploy `envoy` and `milvus ro` pod.

### helm
[helm](https://helm.sh/) can be used to put all in one just as [milvus-helm](https://github.com/milvus-io/milvus-helm)
