![Milvuslogo](https://raw.githubusercontent.com/milvus-io/docs/master/assets/milvus_logo.png)

![LICENSE](https://img.shields.io/badge/license-Apache--2.0-brightgreen)
![Language](https://img.shields.io/badge/language-C%2B%2B-blue)
[![codebeat badge](https://codebeat.co/badges/e030a4f6-b126-4475-a938-4723d54ec3a7?style=plastic)](https://codebeat.co/projects/github-com-jinhai-cn-milvus-master)

![Release](https://img.shields.io/badge/release-v0.5.0-orange)
![Release_date](https://img.shields.io/badge/release_date-October-yellowgreen)

[![codecov](https://codecov.io/gh/milvus-io/milvus/branch/master/graph/badge.svg)](https://codecov.io/gh/milvus-io/milvus)

- [Slack 频道](https://join.slack.com/t/milvusio/shared_invite/enQtNzY1OTQ0NDI3NjMzLWNmYmM1NmNjOTQ5MGI5NDhhYmRhMGU5M2NhNzhhMDMzY2MzNDdlYjM5ODQ5MmE3ODFlYzU3YjJkNmVlNDQ2ZTk)
- [Twitter](https://twitter.com/milvusio)
- [Facebook](https://www.facebook.com/io.milvus.5)
- [博客](https://www.milvus.io/blog/)
- [CSDN](https://zilliz.blog.csdn.net/)
- [中文官网](https://www.milvus.io/zh-CN/)

# 欢迎来到 Milvus

## Milvus 是什么

Milvus 是一款开源的、针对海量特征向量的相似性搜索引擎。基于异构众核计算框架设计，成本更低，性能更好。在有限的计算资源下，十亿向量搜索仅毫秒响应。

Milvus 提供稳定的 [Python](https://github.com/milvus-io/pymilvus)、[Java](https://github.com/milvus-io/milvus-sdk-java) 以及 [C++](https://github.com/milvus-io/milvus/tree/master/core/src/sdk) 的 API 接口。

通过 [版本发布说明](https://milvus.io/docs/zh-CN/release/v0.5.0/) 获取最新发行版本的 Milvus。

- 异构众核

  Milvus 基于异构众核计算框架设计，成本更低，性能更好。

- 多元化索引

  Milvus 支持多种索引方式，使用量化索引、基于树的索引和图索引等算法。

- 资源智能管理

  Milvus 根据实际数据规模和可利用资源，智能调节优化查询计算和索引构建过程。

- 水平扩容

  Milvus 支持在线 / 离线扩容，仅需执行简单命令，便可弹性伸缩计算节点和存储节点。

- 高可用性

  Milvus 集成了 Kubernetes 框架，能有效避免单点障碍情况的发生。

- 简单易用

  Milvus 安装简单，使用方便，并可使您专注于特征向量。

- 可视化监控

  您可以使用基于 Prometheus 的图形化监控，以便实时跟踪系统性能。

## 整体架构

![Milvus_arch](https://github.com/milvus-io/docs/blob/master/assets/milvus_arch.png)

## 开始使用 Milvus

### 硬件要求

| 硬件设备 | 推荐配置                              |
| -------- | ------------------------------------- |
| CPU      | Intel CPU Haswell 及以上              |
| GPU      | NVIDIA Pascal 系列及以上              |
| 内存     | 8 GB 或以上（取决于具体向量数据规模） |
| 硬盘     | SATA 3.0 SSD 及以上                   |

### 使用 Docker

您可以方便地使用 Docker 安装 Milvus。具体请查看 [Milvus 安装指南](https://milvus.io/docs/zh-CN/userguide/install_milvus/)。

### 从源代码编译

#### 软件要求

- Ubuntu 18.04 及以上
- CMake 3.14 及以上
- CUDA 10.0 及以上
- NVIDIA driver 418 及以上

#### 编译

##### 第一步 安装依赖项

```shell
$ cd [Milvus sourcecode path]/core
$ ./ubuntu_build_deps.sh
```

##### 第二步 编译

```shell
$ cd [Milvus sourcecode path]/core
$ ./build.sh -t Debug
or 
$ ./build.sh -t Release
```

当您成功编译后，所有 Milvus 必需组件将安装在`[Milvus root path]/core/milvus`路径下。

##### 启动 Milvus 服务

```shell
$ cd [Milvus root path]/core/milvus
```

在 `LD_LIBRARY_PATH` 中添加 `lib/` 目录：

```shell
$ export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/path/to/milvus/lib
```

启动 Milvus 服务：

```shell
$ cd scripts
$ ./start_server.sh
```

若要停止 Milvus 服务，请使用如下命令：

```shell
$ ./stop_server.sh
```

若需要修改 Milvus 配置文件 `conf/server_config.yaml` 和`conf/log_config.conf`，请查看 [Milvus 配置](https://milvus.io/docs/zh-CN/reference/milvus_config/)。

### 开始您的第一个 Milvus 程序

#### 运行 Python 示例代码

请确保系统的 Python 版本为 [Python 3.5](https://www.python.org/downloads/) 或以上。

安装 Milvus Python SDK。

```shell
# Install Milvus Python SDK
$ pip install pymilvus==0.2.3
```

创建 `example.py` 文件，并向文件中加入 [Python 示例代码](https://github.com/milvus-io/pymilvus/blob/master/examples/advanced_example.py)。

运行示例代码

```shell
# Run Milvus Python example
$ python3 example.py
```

#### 运行 C++ 示例代码

```shell
 # Run Milvus C++ example
 $ cd [Milvus root path]/core/milvus/bin
 $ ./sdk_simple
```

#### 运行 Java 示例代码

请确保系统的 Java 版本为 Java 8 或以上。

请从[此处](https://github.com/milvus-io/milvus-sdk-java/tree/master/examples)获取 Java 示例代码。

## 贡献者指南

我们由衷欢迎您推送贡献。关于贡献流程的详细信息，请参阅 [贡献者指南](https://github.com/milvus-io/milvus/blob/master/CONTRIBUTING.md)。本项目遵循 Milvus [行为准则](https://github.com/milvus-io/milvus/blob/master/CODE_OF_CONDUCT.md)。如果您希望参与本项目，请遵守该准则的内容。

我们使用 [GitHub issues](https://github.com/milvus-io/milvus/issues/new/choose) 追踪问题和补丁。若您希望提出问题或进行讨论，请加入我们的社区。

## 加入 Milvus 社区

欢迎加入我们的 [Slack 频道](https://join.slack.com/t/milvusio/shared_invite/enQtNzY1OTQ0NDI3NjMzLWNmYmM1NmNjOTQ5MGI5NDhhYmRhMGU5M2NhNzhhMDMzY2MzNDdlYjM5ODQ5MmE3ODFlYzU3YjJkNmVlNDQ2ZTk) 以便与其他用户和贡献者进行交流。

## Milvus 路线图

请阅读我们的[路线图](https://milvus.io/docs/zh-CN/roadmap/)以获得更多即将开发的新功能。

## 相关链接

[Milvus 官方网站](https://www.milvus.io/)

[Milvus 文档](https://www.milvus.io/docs/en/userguide/install_milvus/)

[Milvus 在线训练营](https://github.com/milvus-io/bootcamp)

[Milvus 博客](https://www.milvus.io/blog/)

[Milvus CSDN](https://zilliz.blog.csdn.net/)

[Milvus 路线图](https://milvus.io/docs/en/roadmap/)

## 许可协议

[Apache 许可协议2.0版](https://github.com/milvus-io/milvus/blob/master/LICENSE)

