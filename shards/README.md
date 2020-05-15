# Mishards - An Experimental Sharding Middleware 

[中文版](README_CN.md)

Milvus aims to achieve efficient similarity search and analytics for massive-scale vectors. A standalone Milvus instance can easily handle vector search among billion-scale vectors. However, for 10 billion, 100 billion or even larger datasets, a Milvus cluster is needed. 

Ideally, this cluster can be accessed and used just as the standalone instance, meanwhile it satisfies the business requirements such as low latency and high concurrency.

This page meant to demonstrates how to use Mishards, an experimental sharding middleware for Milvus, to establish an orchestrated cluster.

## What is Mishards

Mishards is a middleware that is developed using Python. It provides unlimited extension of memory and computation capacity through request forwarding, read/write splitting, horizontal scalability and dynamic extension. It works as the proxy of the Milvus system. 

Using Mishards in Milvus cluster deployment is an experimental feature available for user test and feedback.

## How Mishards works

Mishards splits the upstream requests to sub-requests and forwards them to Milvus servers. When the search computation is completed, all results are collected by Mishards and sent back to the client. 

Below graph is a demonstration of the process:

![mishards](https://raw.githubusercontent.com/milvus-io/docs/master/assets/mishards.png)

## Mishards example codes

Below examples codes demonstrate how to build from source code a Milvus server with Mishards on a standalone machine, as well as how to use Kubernetes to establish Milvus cluster with Mishards. 

Before executing these examples, make sure you meet the prerequisites of [Milvus installation](https://milvus.io/docs/guides/get_started/install_milvus/install_milvus.md). 

### Build from source code

#### Prequisites

Make sure Python 3.6 or higher is installed.

#### Start Milvus and Mishards from source code

Follow below steps to start a standalone Milvus instance with Mishards from source code:

1. Clone milvus repository.

   ```shell
   git clone <milvus repo http/ssh url>
   ```

2. Install Mishards dependencies.

   ```shell
   $ cd milvus/shards
   $ pip install -r requirements.txt
   ```

3. Start Milvus server.

   ```shell
   $ sudo nvidia-docker run --rm -d -p 19530:19530 -v /tmp/milvus/db:/var/lib/milvus/db milvusdb/milvus:0.9.0-gpu-d051520-cb92b1
   ```

4. Update path permissions.

   ```shell
   $ sudo chown -R $USER:$USER /tmp/milvus
   ```

5. Configure Mishards environmental variables.

   ```shell
   $ cp mishards/.env.example mishards/.env
   ```

6. Start Mishards server.

   ```shell
   $ python mishards/main.py
   ```

### Docker example 

The `all_in_one` example shows how to use Docker container to start 2 Milvus instances, 1 Mishards instance and 1 Jaeger instance.  

 1. Install [Docker Compose](https://docs.docker.com/compose/install/).

 2. Build docker images for these instances. 

    ```shell
    $ make build
    ```

 3. Start all instances.

    ```shell
    $ make deploy
    ```

 4. Confirm instance status. 

    ```shell
    $ make probe_deploy
         Pass ==> Pass: Connected
         Fail ==> Error: Fail connecting to server on 127.0.0.1:19530. Timeout
    ```

To check the service tracing, open the [Jaeger page](http://127.0.0.1:16686/) on your browser. 

![jaegerui](https://raw.githubusercontent.com/milvus-io/docs/master/assets/jaegerui.png)

![jaegertraces](https://raw.githubusercontent.com/milvus-io/docs/master/assets/jaegertraces.png)

To stop all instances, use the following command:

```shell
$ make clean_deploy
```

### Kubernetes example

Using Kubernetes to deploy Milvus cluster requires that the developers have a basic understanding of [general concepts](https://kubernetes.io/docs/concepts/) of Kubernetes. 

This example mainly demonstrates how to use Kubernetes to establish a Milvus cluster containing 2 Milvus instances（1 read instance and 1 write instance), 1 MySQL instance and 1 Mishards instance. 

This example does not include tasks such as setting up Kubernetes cluster, [installing shared storage](https://kubernetes.io/docs/concepts/storage/volumes/) and using command tools such as [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/).

Below is the architecture of Milvus cluster built upon Kubernetes:

![k8s_arch](https://raw.githubusercontent.com/milvus-io/docs/master/assets/k8s_arch.png)

#### Prerequisites

- A Kubernetes cluster is already established.
- [nvidia-docker 2.0](https://github.com/nvidia/nvidia-docker/wiki/Installation-(version-2.0)) is already installed.
- Shared storage is already installed. 
- kubectl is installed and can access the Kubernetes cluster.

#### Use Kubernetes to build a Milvus cluster

1. Start Milvus cluster

   ```shell
   $ make cluster
   ```

2. Confirm that Mishards is connected to Milvus.

   ```shell
   $ make probe_cluster
     Pass ==> Pass: Connected
   ```

To check cluster status:

```shell
$ make cluster_status
```

To delete the cluster: 

```shell
$ make clean_cluster
```

To add a read instance:

```shell
$ cd kubernetes_demo
$ ./start.sh scale-ro-server 2 
```

To add a proxy instance: 

```shell
$ cd kubernetes_demo
$ ./start.sh scale-proxy 2 
```

To check cluster logs: 

```shell
$ kubectl logs -f --tail=1000 -n milvus milvus-ro-servers-0 
```

## Mishards Unit test

**Unit test**

```shell
$ cd milvus/shards
$ make test
```

**Code coverage test**

```shell
$ cd milvus/shards
$ make coverage
```

**Code format check**

```shell
$ cd milvus/shards
$ make style
```

## Mishards configuration

### Overall configuration

| Name          | Required | Type    | Default | Description                                                  |
| ------------- | -------- | ------- | ------- | ------------------------------------------------------------ |
| `Debug`       | No       | boolean | `True`  | Choose if to enable `Debug` work mode.                       |
| `TIMEZONE`    | No       | string  | `UTC`   | Timezone                                                     |
| `MAX_RETRY`   | No       | integer | `3`     | The maximum retry times allowed to connect to Milvus.        |
| `SERVER_PORT` | No       | integer | `19530` | Define the server port of Mishards.                          |
| `WOSERVER`    | **Yes**  | string  | ` `     | Define the address of Milvus write instance. Currently, only static settings are supported. Format for reference: `tcp://127.0.0.1:19530`. |

### Metadata

| Name                           | Required | Type    | Default | Description                                                  |
| ------------------------------ | -------- | ------- | ------- | ------------------------------------------------------------ |
| `SQLALCHEMY_DATABASE_URI`      | **Yes**  | string  | ` `     | Define the database address for metadata storage. Format standard: RFC-738-style. For example: `mysql+pymysql://root:root@127.0.0.1:3306/milvus?charset=utf8mb4`. |
| `SQL_ECHO`                     | No       | boolean | `False` | Choose if to print SQL statements.                           |
| `SQLALCHEMY_DATABASE_TEST_URI` | No       | string  | ` `     | Define the database address of metadata storage in test environment. |
| `SQL_TEST_ECHO`                | No       | boolean | `False` | Choose if to print SQL statements in test environment.       |

### Service discovery

| Name                                  | Required | Type    | Default       | Description                                                  |
| ------------------------------------- | -------- | ------- | ------------- | ------------------------------------------------------------ |
| `DISCOVERY_PLUGIN_PATH`               | No       | string  | ` `           | Define the search path to locate the plug-in. The default path is used if the value is not set. |
| `DISCOVERY_CLASS_NAME`                | No       | string  | `static`      | Under the plug-in search path, search the class based on the class name, and instantiate it. Currently, the system provides 2 classes:  `static` and `kubernetes`. |
| `DISCOVERY_STATIC_HOSTS`              | No       | list    | `[]`          | When `DISCOVERY_CLASS_NAME` is `static` , define a comma-separated service address list, for example`192.168.1.188,192.168.1.190`. |
| `DISCOVERY_STATIC_PORT`               | No       | integer | `19530`       | When `DISCOVERY_CLASS_NAME` is `static`, define the server port. |
| `DISCOVERY_KUBERNETES_NAMESPACE`      | No       | string  | ` `           | When `DISCOVERY_CLASS_NAME` is `kubernetes`, define the namespace of Milvus cluster. |
| `DISCOVERY_KUBERNETES_IN_CLUSTER`     | No       | boolean | `False`       | When `DISCOVERY_CLASS_NAME` is `kubernetes` , choose if to run the server in Kubernetes. |
| `DISCOVERY_KUBERNETES_POLL_INTERVAL`  | No       | integer | `5` (Seconds) | When `DISCOVERY_CLASS_NAME` is `kubernetes` , define the listening cycle of the server. |
| `DISCOVERY_KUBERNETES_POD_PATT`       | No       | string  | ` `           | When `DISCOVERY_CLASS_NAME` is `kubernetes` , map the regular expression of Milvus Pod. |
| `DISCOVERY_KUBERNETES_LABEL_SELECTOR` | No       | string  | ` `           | When `SD_PROVIDER` is `kubernetes`, map the label of Milvus Pod. For example: `tier=ro-servers`. |

### Tracing

| Name                    | Required | Type    | Default    | Description                                                  |
| ----------------------- | -------- | ------- | ---------- | ------------------------------------------------------------ |
| `TRACER_PLUGIN_PATH`    | No       | string  | ` `        | Define the search path to locate the  tracing plug-in. The default path is used if the value is not set. |
| `TRACER_CLASS_NAME`     | No       | string  | ` `        | Under the plug-in search path, search the class based on the class name, and instantiate it. Currently, only `Jaeger` is supported. |
| `TRACING_SERVICE_NAME`  | No       | string  | `mishards` | When `TRACING_CLASS_NAME` is [`Jaeger`](https://www.jaegertracing.io/docs/1.14/), the name of the tracing service. |
| `TRACING_SAMPLER_TYPE`  | No       | string  | `const`    | When `TRACING_CLASS_NAME` is [`Jaeger`](https://www.jaegertracing.io/docs/1.14/), the [sampling type](https://www.jaegertracing.io/docs/1.14/sampling/) of the tracing service. |
| `TRACING_SAMPLER_PARAM` | No       | integer | `1`        | When `TRACING_CLASS_NAME` is [`Jaeger`](https://www.jaegertracing.io/docs/1.14/), the [sampling frequency](https://www.jaegertracing.io/docs/1.14/sampling/) of the tracing service. |
| `TRACING_LOG_PAYLOAD`   | No       | boolean | `False`    | When `TRACING_CLASS_NAME` is [`Jaeger`](https://www.jaegertracing.io/docs/1.14/), choose if to sample Payload. |

### Logging

| Name        | Required | Type   | Default         | Description                                                  |
| ----------- | -------- | ------ | --------------- | ------------------------------------------------------------ |
| `LOG_LEVEL` | No       | string | `DEBUG`         | Log recording levels. Currently supports `DEBUG` ,`INFO` ,`WARNING` and `ERROR`. |
| `LOG_PATH`  | No       | string | `/tmp/mishards` | Log recording path.                                          |
| `LOG_NAME`  | No       | string | `logfile`       | Log recording name.                                          |

### Routing

| Name                     | Required | Type   | Default                   | Description                                                  |
| ------------------------ | -------- | ------ | ------------------------- | ------------------------------------------------------------ |
| `ROUTER_PLUGIN_PATH`     | No       | string | ` `                       | Define the search path to locate the routing plug-in. The default path is used if the value is not set. |
| `ROUTER_CLASS_NAME`      | No       | string | `FileBasedHashRingRouter` | Under the plug-in search path, search the class based on the class name, and instantiate it. Currently, only `FileBasedHashRingRouter` is supported. |
| `ROUTER_CLASS_TEST_NAME` | No       | string | `FileBasedHashRingRouter` | Under the plug-in search path, search the class based on the class name, and instantiate it. Currently, `FileBasedHashRingRouter` is supported for test environment only. |

