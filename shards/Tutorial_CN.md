# Mishards使用文档
---
Milvus 旨在帮助用户实现海量非结构化数据的近似检索和分析。单个 Milvus 实例可处理十亿级数据规模，而对于百亿或者千亿规模数据的需求，则需要一个 Milvus 集群实例，该实例对于上层应用可以像单机实例一样使用，同时满足海量数据低延迟，高并发业务需求。mishards就是一个集群中间件，其内部处理请求转发，读写分离，水平扩展，动态扩容，为用户提供内存和算力可以无限扩容的 Milvus 实例。

## 运行环境
---

### 单机快速启动实例
**`python >= 3.4`环境**

```
1. cd milvus/shards
2. pip install -r requirements.txt
3. nvidia-docker run --rm -d -p 19530:19530 -v /tmp/milvus/db:/opt/milvus/db milvusdb/milvus:0.5.0-d102119-ede20b
4. sudo chown -R $USER:$USER /tmp/milvus
5. cp mishards/.env.example mishards/.env
6
7. 在python mishards/main.py #.env配置mishards监听19532端口
```

### 容器启动实例
`all_in_one`会在服务器上开启两个milvus实例，一个mishards实例，一个jaeger链路追踪实例

**启动**
```
1. 安装docker-compose
1. cd milvus/shards/all_in_one
2. docker-compose -f all_in_one.yml up -d #监听19531端口
```

**打开Jaeger UI**
```
浏览器打开 "http://127.0.0.1:16686/"
```

### kubernetes中快速启动
**准备**
```
- kubernetes集群
- 安装nvidia-docker
- 共享存储
- 安装kubectl并能访问集群
```

**步骤**
```
1. cd milvus/shards/kubernetes_demo/
2. ./start.sh allup
3. watch -n 1 kubectl get pods -n milvus -o wide 查看所有pod状态，等待所有pod都处于Runing状态
4. kubectl get service -n milvus 查看milvus-proxy-servers的EXTERNAL-IP和PORT, 这就是mishards集群的服务地址
```

**扩容计算实例**
```
./start.sh scale-ro-server 2 扩容计算实例到2
```

**扩容代理器实例**
```
./start.sh scale-proxy 2 扩容代理服务器实例到2
```

**查看日志**
```
kubectl logs -f --tail=1000 -n milvus milvus-ro-servers-0 查看计算节点milvus-ro-servers-0日志
```

## 测试

**启动单元测试**
```
1. cd milvus/shards
2. pytest
```

**单元测试覆盖率**
```
pytest --cov-report html:cov_html --cov=mishards
```

## mishards配置详解

### 全局
| Name | Required  | Type | Default Value | Explanation |
| --------------------------- | -------- | -------- | ------------- | ------------- |
| Debug | No | bool | True | 是否Debug工作模式 |
| TIMEZONE | No | string | "UTC" | 时区 |
| MAX_RETRY | No | int | 3 | 最大连接重试次数 |
| SERVER_PORT | No | int | 19530 | 配置服务端口 |
| WOSERVER | **Yes** | str | - | 配置后台可写Milvus实例地址。目前只支持静态设置,例"tcp://127.0.0.1:19530" |

### 元数据
| Name | Required  | Type | Default Value | Explanation |
| --------------------------- | -------- | -------- | ------------- | ------------- |
| SQLALCHEMY_DATABASE_URI | **Yes** | string | - | 配置元数据存储数据库地址 |
| SQL_ECHO | No | bool | False | 是否打印Sql详细语句 |
| SQLALCHEMY_DATABASE_TEST_URI | No | string | - | 配置测试环境下元数据存储数据库地址 |
| SQL_TEST_ECHO | No | bool | False | 配置测试环境下是否打印Sql详细语句 |

### 服务发现
| Name | Required  | Type | Default Value | Explanation |
| --------------------------- | -------- | -------- | ------------- | ------------- |
| DISCOVERY_PLUGIN_PATH | No | string | - | 用户自定义服务发现插件搜索路径，默认使用系统搜索路径|
| DISCOVERY_CLASS_NAME | No | string | static | 在服务发现插件搜索路径下搜索类并实例化。目前系统提供 **static** 和 **kubernetes** 两种类，默认使用 **static** |
| DISCOVERY_STATIC_HOSTS | No | list | [] | **DISCOVERY_CLASS_NAME** 为 **static** 时，配置服务地址列表，例"192.168.1.188,192.168.1.190"|
| DISCOVERY_STATIC_PORT | No | int | 19530 | **DISCOVERY_CLASS_NAME** 为 **static** 时，配置 Hosts 监听端口 |
| DISCOVERY_KUBERNETES_NAMESPACE | No | string | - | **DISCOVERY_CLASS_NAME** 为 **kubernetes** 时，配置集群 namespace |
| DISCOVERY_KUBERNETES_IN_CLUSTER | No | bool | False | **DISCOVERY_CLASS_NAME** 为 **kubernetes** 时，标明服务发现是否在集群中运行 |
| DISCOVERY_KUBERNETES_POLL_INTERVAL | No | int | 5 | **DISCOVERY_CLASS_NAME** 为 **kubernetes** 时，标明服务发现监听服务列表频率,单位 Second |
| DISCOVERY_KUBERNETES_POD_PATT | No | string | - | **DISCOVERY_CLASS_NAME** 为 **kubernetes** 时，匹配可读 Milvus 实例的正则表达式 |
| DISCOVERY_KUBERNETES_LABEL_SELECTOR | No | string | - | **SD_PROVIDER** 为**Kubernetes**时，匹配可读Milvus实例的标签选择 |

### 链路追踪
| Name | Required  | Type | Default Value | Explanation |
| --------------------------- | -------- | -------- | ------------- | ------------- |
| TRACER_PLUGIN_PATH | No | string | - | 用户自定义链路追踪插件搜索路径，默认使用系统搜索路径|
| TRACER_CLASS_NAME | No | string | "" | 链路追踪方案选择，目前只实现 **Jaeger**, 默认不使用|
| TRACING_SERVICE_NAME | No | string | "mishards" | **TRACING_TYPE** 为 **Jaeger** 时，链路追踪服务名 |
| TRACING_SAMPLER_TYPE | No | string | "const" | **TRACING_TYPE** 为 **Jaeger** 时，链路追踪采样类型 |
| TRACING_SAMPLER_PARAM | No | int | 1 | **TRACING_TYPE** 为 **Jaeger** 时，链路追踪采样频率 |
| TRACING_LOG_PAYLOAD | No | bool | False | **TRACING_TYPE** 为 **Jaeger** 时，链路追踪是否采集 Payload |

### 日志
| Name | Required  | Type | Default Value | Explanation |
| --------------------------- | -------- | -------- | ------------- | ------------- |
| LOG_LEVEL | No | string | "DEBUG" if Debug is ON else "INFO" | 日志记录级别 |
| LOG_PATH | No | string | "/tmp/mishards" | 日志记录路径 |
| LOG_NAME | No | string | "logfile" | 日志记录名 |

### 路由
| Name | Required  | Type | Default Value | Explanation |
| --------------------------- | -------- | -------- | ------------- | ------------- |
| ROUTER_PLUGIN_PATH | No | string | - | 用户自定义路由插件搜索路径，默认使用系统搜索路径|
| ROUTER_CLASS_NAME | No | string | FileBasedHashRingRouter | 处理请求路由类名, 可注册自定义类。目前系统只提供了类 **FileBasedHashRingRouter** |
| ROUTER_CLASS_TEST_NAME | No | string | FileBasedHashRingRouter | 测试环境下处理请求路由类名, 可注册自定义类 |
