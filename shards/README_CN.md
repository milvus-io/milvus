# Mishards - Milvus 集群分片中间件

Milvus 旨在帮助用户实现海量非结构化数据的近似检索和分析。单个 Milvus 实例可处理十亿级数据规模，而对于百亿或者千亿级数据，则需要一个 Milvus 集群实例。该实例对于上层应用可以像单机实例一样使用，同时满足海量数据低延迟、高并发业务需求。

本文主要展示如何使用 Mishards 分片中间件来搭建 Milvus 集群。

## Mishards 是什么

Mishards 是一个用 Python 开发的 Milvus 集群分片中间件，其内部处理请求转发、读写分离、水平扩展、动态扩容，为用户提供内存和算力可以无限扩容的 Milvus 实例。

Mishards 的设计尚未完成，属于试用功能，希望大家多多测试、提供反馈。

## Mishards 如何工作

Mishards 负责将上游请求拆分，并路由到内部各细分子服务，最后将子服务结果汇总，返回给上游。

![mishards](https://raw.githubusercontent.com/milvus-io/docs/master/assets/mishards.png)

## Mishards 相关示例

以下分别向您展示如何使用源代码在单机上启动 Mishards 和 Milvus 服务，以及如何使用 Kubernetes 启动 Milvus 集群和 Mishards。

Milvus 启动的前提条件请参考 [Milvus 安装](https://milvus.io/cn/docs/guides/get_started/install_milvus/install_milvus.md)。

### 源代码启动示例

#### 前提条件

Python 版本为3.6及以上。

#### 源代码启动 Milvus 和 Mishards 实例

请按照以下步骤在单机上启动单个 Milvus 实例和 Mishards 服务：

1. 将 milvus repository 复制到本地。

   ```shell
   git clone <ssh url>
   ```

2. 安装 Mishards 的依赖库。

   ```shell
   $ cd milvus/shards
   $ pip install -r requirements.txt
   ```

3. 启动 Milvus 服务。

   ```shell
   $ sudo nvidia-docker run --rm -d -p 19530:19530 -v /tmp/milvus/db:/var/lib/milvus/db milvusdb/milvus:0.9.0-gpu-d051520-cb92b1
   ```

4. 更改目录权限。

   ```shell
   $ sudo chown -R $USER:$USER /tmp/milvus
   ```

5. 配置 Mishards 环境变量

   ```shell
   $ cp mishards/.env.example mishards/.env
   ```

6. 启动 Mishards 服务

   ```shell
   $ python mishards/main.py
   ```

### Docker 示例

`all_in_one` 使用 Docker 容器启动2个 Milvus 实例，1个 Mishards 中间件实例，和1个 Jaeger 链路追踪实例。

 1. 安装 [Docker Compose](https://docs.docker.com/compose/install/)。

 2. 制作实例镜像。

    ```shell
    $ make build
    ```

 3. 启动所有服务。

    ```shell
    $ make deploy
    ```

 4. 检查确认服务状态。

    ```shell
    $ make probe_deploy
         Pass ==> Pass: Connected
         Fail ==> Error: Fail connecting to server on 127.0.0.1:19530. Timeout
    ```

若要查看服务踪迹，使用浏览器打开 [Jaeger 页面](http://127.0.0.1:16686/)。

![jaegerui](https://github.com/milvus-io/docs/blob/master/assets/jaegerui.png)

![jaegertraces](https://github.com/milvus-io/docs/blob/master/assets/jaegertraces.png)

若要清理所有服务，请使用如下命令：

```shell
$ make clean_deploy
```

### Kubernetes 示例

使用 Kubernetes 部署 Milvus 分布式集群要求开发人员对 Kubernetes 的[基本概念](https://kubernetes.io/docs/concepts/)和操作有基本了解。

本示例主要展示如何使用 Kubernetes 搭建 Milvus 集群，包含2个 Milvus 实例（1个可读实例，1个可写实例）、1个 MySQL 实例和1个 Mishards 实例。

本示例不包括如何搭建 Kubernetes 集群，如何安装[共享存储](https://kubernetes.io/docs/concepts/storage/volumes/)和如何安装 [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/) 命令行工具等。

以下是 Kubernetes 示例架构图：

![k8s_arch](https://github.com/milvus-io/docs/blob/master/assets/k8s_arch.png)

#### 前提条件

使用 Kubernetes 启动多个 Milvus 实例之前，请确保您已满足以下条件：

- 已创建 Kubernetes 集群
- 已安装 [nvidia-docker 2.0](https://github.com/nvidia/nvidia-docker/wiki/Installation-(version-2.0))
- 已安装共享存储
- 已安装 kubectl，且能访问集群

#### Kubernetes 启动集群

1. 启动 Milvus 集群。

   ```shell
   $ make cluster
   ```

2. 确认 Mishards 是否可用。

   ```shell
   $ make probe_cluster
     Pass ==> Pass: Connected
   ```

查看集群状态：

```shell
$ make cluster_status
```

删除 Milvus 集群：

```shell
$ make clean_cluster
```

扩容 Milvus 可读实例到2个：

```shell
$ cd kubernetes_demo
$ ./start.sh scale-ro-server 2 
```

扩容 Mishards（代理）实例到2个：

```shell
$ cd kubernetes_demo
$ ./start.sh scale-proxy 2 
```

查看计算节点 `milvus-ro-servers-0` 日志：

```shell
$ kubectl logs -f --tail=1000 -n milvus milvus-ro-servers-0 
```

## 单元测试

**单元测试**

```shell
$ cd milvus/shards
$ make test
```

**代码覆盖率测试**

```shell
$ cd milvus/shards
$ make coverage
```

**代码格式检查**

```shell
$ cd milvus/shards
$ make style
```

## Mishards 配置

### 全局配置

| 参数          | 是否必填 | 类型    | 默认值  | 说明                                                         |
| ------------- | -------- | ------- | ------- | ------------------------------------------------------------ |
| `Debug`       | No       | boolean | `True`  | 选择是否启用 `Debug` 工作模式。                              |
| `TIMEZONE`    | No       | string  | `UTC`   | 时区                                                         |
| `MAX_RETRY`   | No       | integer | `3`     | Mishards 连接 Milvus 的最大重试次数。                        |
| `SERVER_PORT` | No       | integer | `19530` | 定义 Mishards 的服务端口。                                   |
| `WOSERVER`    | **Yes**  | string  | ` `     | 定义 Milvus 可写实例的地址，目前只支持静态设置。参考格式： `tcp://127.0.0.1:19530`。 |

### 元数据

| 参数                           | 是否必填 | 类型    | 默认值  | 说明                                                         |
| ------------------------------ | -------- | ------- | ------- | ------------------------------------------------------------ |
| `SQLALCHEMY_DATABASE_URI`      | **Yes**  | string  | ` `     | 定义元数据存储的数据库地址，格式标准为 RFC-738-style。例如：`mysql+pymysql://root:root@127.0.0.1:3306/milvus?charset=utf8mb4`。 |
| `SQL_ECHO`                     | No       | boolean | `False` | 选择是否打印 SQL 详细语句。                                  |
| `SQLALCHEMY_DATABASE_TEST_URI` | No       | string  | ` `     | 定义测试环境下元数据存储的数据库地址。                       |
| `SQL_TEST_ECHO`                | No       | boolean | `False` | 选择测试环境下是否打印 SQL 详细语句。                        |

### 服务发现

| 参数                                  | 是否必填 | 类型    | 默认值   | 说明                                                         |
| ------------------------------------- | -------- | ------- | -------- | ------------------------------------------------------------ |
| `DISCOVERY_PLUGIN_PATH`               | No       | string  | ` `      | 用户自定义服务发现插件的搜索路径，默认使用系统搜索路径。     |
| `DISCOVERY_CLASS_NAME`                | No       | string  | `static` | 在插件搜索路径下，根据类名搜索类，并将其实例化。目前系统提供 `static` 和 `kubernetes` 两种类，默认使用 `static`。 |
| `DISCOVERY_STATIC_HOSTS`              | No       | list    | `[]`     | `DISCOVERY_CLASS_NAME`为 `static` 时，定义服务地址列表，地址之间以逗号隔开，例如 `192.168.1.188,192.168.1.190`。 |
| `DISCOVERY_STATIC_PORT`               | No       | integer | `19530`  | `DISCOVERY_CLASS_NAME` 为 `static` 时，定义服务地址监听端口。 |
| `DISCOVERY_KUBERNETES_NAMESPACE`      | No       | string  | ` `      | `DISCOVERY_CLASS_NAME` 为 `kubernetes`时，定义 Milvus 集群的namespace。 |
| `DISCOVERY_KUBERNETES_IN_CLUSTER`     | No       | boolean | `False`  | `DISCOVERY_CLASS_NAME` 为 `kubernetes` 时，选择服务发现是否在集群中运行。 |
| `DISCOVERY_KUBERNETES_POLL_INTERVAL`  | No       | integer | `5`      | `DISCOVERY_CLASS_NAME` 为 `kubernetes` 时，定义服务发现监听周期，单位：second。 |
| `DISCOVERY_KUBERNETES_POD_PATT`       | No       | string  | ` `      | `DISCOVERY_CLASS_NAME` 为 `kubernetes` 时，匹配 Milvus Pod 名字的正则表达式。 |
| `DISCOVERY_KUBERNETES_LABEL_SELECTOR` | No       | string  | ` `      | `SD_PROVIDER`为`kubernetes`时，匹配 Milvus Pod 的标签。例如：`tier=ro-servers`。 |

### 链路追踪

| 参数                    | 是否必填 | 类型    | 默认值     | 说明                                                         |
| ----------------------- | -------- | ------- | ---------- | ------------------------------------------------------------ |
| `TRACER_PLUGIN_PATH`    | No       | string  | ` `        | 用户自定义链路追踪插件的搜索路径，默认使用系统搜索路径。     |
| `TRACER_CLASS_NAME`     | No       | string  | ` `        | 在插件搜索路径下，根据类名搜索类，并将其实例化。目前只支持 `Jaeger`, 默认不使用。 |
| `TRACING_SERVICE_NAME`  | No       | string  | `mishards` | `TRACING_CLASS_NAME` 为 [`Jaeger`](https://www.jaegertracing.io/docs/1.14/)时，链路追踪的 service。 |
| `TRACING_SAMPLER_TYPE`  | No       | string  | `const`    | `TRACING_CLASS_NAME`为 `Jaeger` 时，链路追踪的[采样类型](https://www.jaegertracing.io/docs/1.14/sampling/)。 |
| `TRACING_SAMPLER_PARAM` | No       | integer | `1`        | `TRACING_CLASS_NAME` 为 `Jaeger`时，链路追踪的[采样频率](https://www.jaegertracing.io/docs/1.14/sampling/)。 |
| `TRACING_LOG_PAYLOAD`   | No       | boolean | `False`    | `TRACING_CLASS_NAME`为 `Jaeger`时，链路追踪是否采集 Payload。 |

### 日志

| 参数        | 是否必填 | 类型   | 默认值          | 说明                                                         |
| ----------- | -------- | ------ | --------------- | ------------------------------------------------------------ |
| `LOG_LEVEL` | No       | string | `DEBUG`         | 日志记录级别，目前支持 `DEBUG` 、`INFO` 、`WARNING` 和`ERROR`。 |
| `LOG_PATH`  | No       | string | `/tmp/mishards` | 日志记录路径。                                               |
| `LOG_NAME`  | No       | string | `logfile`       | 日志记录名。                                                 |

### 路由

| 参数                     | 是否必填 | 类型   | 默认值                    | 说明                                                         |
| ------------------------ | -------- | ------ | ------------------------- | ------------------------------------------------------------ |
| `ROUTER_PLUGIN_PATH`     | No       | string | ` `                       | 用户自定义路由插件的搜索路径，默认使用系统搜索路径。         |
| `ROUTER_CLASS_NAME`      | No       | string | `FileBasedHashRingRouter` | 在插件搜索路径下，根据类名搜索路由的类，并将其实例化。目前系统只提供了 `FileBasedHashRingRouter`。 |
| `ROUTER_CLASS_TEST_NAME` | No       | string | `FileBasedHashRingRouter` | 在插件搜索路径下，根据类名搜索路由的类，并将其实例化。目前系统只提供了 `FileBasedHashRingRouter`，仅限测试环境下使用。 |
