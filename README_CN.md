![Milvuslogo](https://github.com/milvus-io/docs/blob/master/v0.9.1/assets/milvus_logo.png)
[![Slack](https://img.shields.io/badge/Join-Slack-orange)](https://join.slack.com/t/milvusio/shared_invite/zt-e0u4qu3k-bI2GDNys3ZqX1YCJ9OM~GQ)

![GitHub](https://img.shields.io/github/license/milvus-io/milvus)
![Docker pulls](https://img.shields.io/docker/pulls/milvusdb/milvus)

[![Build Status](http://internal.zilliz.com:18080/jenkins/job/milvus-ci/job/master/badge/icon)](http://internal.zilliz.com:18080/jenkins/job/milvus-ci/job/master/)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/3563/badge)](https://bestpractices.coreinfrastructure.org/projects/3563)
[![codecov](https://codecov.io/gh/milvus-io/milvus/branch/master/graph/badge.svg)](https://codecov.io/gh/milvus-io/milvus)
[![codebeat badge](https://codebeat.co/badges/e030a4f6-b126-4475-a938-4723d54ec3a7?style=plastic)](https://codebeat.co/projects/github-com-milvus-io-milvus-master)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/c4bb2ccfb51b47f99e43bfd1705edd95)](https://app.codacy.com/gh/milvus-io/milvus?utm_source=github.com&utm_medium=referral&utm_content=milvus-io/milvus&utm_campaign=Badge_Grade_Dashboard)

[English](README.md) | 中文版  

# 欢迎来到 Milvus

## Milvus 是什么

Milvus 是一款开源的向量相似度搜索引擎，支持针对 TB 级向量的增删改操作和近实时查询，具有高度灵活、稳定可靠以及高速查询等特点。Milvus 集成了 Faiss、NMSLIB、Annoy 等广泛应用的向量索引库，提供了一整套简单直观的 API，让你可以针对不同场景选择不同的索引类型。此外，Milvus 还可以对标量数据进行过滤，进一步提高了召回率，增强了搜索的灵活性。

Milvus 的架构如下：

![arch](https://github.com/milvus-io/docs/blob/master/v0.9.1/assets/milvus_arch.png)

更多 Milvus 相关介绍和整体架构信息，详见 [Milvus 简介](https://www.milvus.io/cn/docs/overview.md)。你可以通过 [版本发布说明](https://www.milvus.io/cn/docs/release_notes.md) 获取最新版本的功能和更新。

Milvus 在 Apache 2 License 协议下发布，于 2019 年 10 月正式开源，是 [LF AI & DATA 基金会](https://lfaidata.foundation/)的孵化项目。Milvus 的源代码被托管于 [Github](https://github.com/milvus-io/milvus)。

## Milvus 快速上手

### 安装 Milvus

- 使用 Docker 容器安装 Milvus，详见 [Milvus 安装指南](https://www.milvus.io/cn/docs/install_milvus.md)。
- 基于源码编译，详见[源码安装](INSTALL.md)。

### 试用示例代码

你可以尝试用 [Python](https://www.milvus.io/docs/example_code.md)，[Java](https://github.com/milvus-io/milvus-sdk-java/tree/master/examples)，[Go](https://github.com/milvus-io/milvus-sdk-go/tree/master/examples)，或者 [C++](https://github.com/milvus-io/milvus/tree/master/sdk/examples) 运行 Milvus 示例代码。

## 支持的客户端

-   [Go](https://github.com/milvus-io/milvus-sdk-go)
-   [Python](https://github.com/milvus-io/pymilvus)
-   [Java](https://github.com/milvus-io/milvus-sdk-java)
-   [C++](https://github.com/milvus-io/milvus/tree/master/sdk)
-   [RESTful API](https://github.com/milvus-io/milvus/tree/master/core/src/server/web_impl)
-   [Node.js](https://www.npmjs.com/package/@arkie-ai/milvus-client) (由 [arkie](https://www.arkie.cn/) 提供)

## 应用场景

Milvus 已经广泛应用于多个领域，在全球范围内上被数百家组织和机构采用:

- 图像、视频、音频等音视频搜索领域
- 文本搜索、推荐和交互式问答系统等文本搜索领域
- 新药搜索、基因筛选等生物医药领域

体验在线场景展示，详见 [Milvus 应用场景](https://milvus.io/cn/scenarios)。

了解更详细的应用场景和解决方案，详见 [Milvus 训练营](https://github.com/milvus-io/bootcamp)。

## 性能基准测试

更多 Milvus 性能基准的信息，详见[测试报告](https://github.com/milvus-io/bootcamp/tree/master/benchmark_test)。

## 路线图

了解 Milvus 即将实现的新特性，详见[路线图](https://github.com/milvus-io/milvus/milestones)，

路线图尚未完成，未来会持续进行合理改动。欢迎提出相关意见、需求和建议。

## 加入开发者社区

我们由衷期盼你加入开发者社区，成为本项目的贡献者。

- 如果你想为 Milvus 项目贡献代码，详见[贡献者指南](https://github.com/milvus-io/milvus/blob/master/CONTRIBUTING.md)。
- 贡献者应遵循 Milvus [行为准则](https://github.com/milvus-io/milvus/blob/master/CODE_OF_CONDUCT.md)参与本项目。
- 使用 [GitHub issues](https://github.com/milvus-io/milvus/issues) 追踪问题和补丁。
- 如果你对 Milvus 有任何与功能、SDK 等相关的问题，欢迎加入 [Slack](https://join.slack.com/t/milvusio/shared_invite/zt-e0u4qu3k-bI2GDNys3ZqX1YCJ9OM~GQ) 参与讨论。

## 加入 Milvus 技术交流微信群

![qrcode](https://github.com/milvus-io/docs/blob/master/v0.11.0/assets/qrcode.png)

## 相关链接

-   [Milvus.io](https://www.milvus.io)

-   [Milvus 常见问题](https://www.milvus.io/cn/docs/operational_faq.md)

-   [Milvus Medium](https://medium.com/@milvusio)

-   [Milvus CSDN](https://zilliz.blog.csdn.net/)

-   [Milvus Twitter](https://twitter.com/milvusio)

-   [Milvus Facebook](https://www.facebook.com/io.milvus.5)

-   [Milvus 设计文档](DESIGN.md)

## 许可协议

[Apache 许可协议 2.0 版](https://github.com/milvus-io/milvus/blob/master/LICENSE)
