![Milvuslogo](https://github.com/milvus-io/docs/blob/master/assets/milvus_logo.png)

[![Slack](https://img.shields.io/badge/Join-Slack-orange)](https://join.slack.com/t/milvusio/shared_invite/enQtNzY1OTQ0NDI3NjMzLWNmYmM1NmNjOTQ5MGI5NDhhYmRhMGU5M2NhNzhhMDMzY2MzNDdlYjM5ODQ5MmE3ODFlYzU3YjJkNmVlNDQ2ZTk)

![GitHub](https://img.shields.io/github/license/milvus-io/milvus)
![Language](https://img.shields.io/github/languages/count/milvus-io/milvus)
![GitHub top language](https://img.shields.io/github/languages/top/milvus-io/milvus)
![GitHub release (latest by date)](https://img.shields.io/github/v/release/milvus-io/milvus)
![GitHub Release Date](https://img.shields.io/github/release-date/milvus-io/milvus)

[![Maintenance](https://img.shields.io/badge/Maintained%3F-yes-green.svg)](https://github.com/milvus-io/milvus/pulse/monthly)
![OSS Lifecycle](https://img.shields.io/osslifecycle/milvus-io/milvus)
[![HitCount](http://hits.dwyl.com/milvus-io/milvus.svg)](http://hits.dwyl.com/milvus-io/milvus)
![Docker pulls](https://img.shields.io/docker/pulls/milvusdb/milvus)

[![Build Status](http://internal.zilliz.com:18080/jenkins/job/milvus-ci/job/master/badge/icon)](http://internal.zilliz.com:18080/jenkins/job/milvus-ci/job/master/)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/3563/badge)](https://bestpractices.coreinfrastructure.org/projects/3563)
[![codecov](https://codecov.io/gh/milvus-io/milvus/branch/master/graph/badge.svg)](https://codecov.io/gh/milvus-io/milvus)
[![codebeat badge](https://codebeat.co/badges/e030a4f6-b126-4475-a938-4723d54ec3a7?style=plastic)](https://codebeat.co/projects/github-com-milvus-io-milvus-master)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/c4bb2ccfb51b47f99e43bfd1705edd95)](https://app.codacy.com/gh/milvus-io/milvus?utm_source=github.com&utm_medium=referral&utm_content=milvus-io/milvus&utm_campaign=Badge_Grade_Dashboard)
[![Scrutinizer Quality Score](https://scrutinizer-ci.com/g/milvus-io/milvus/badges/quality-score.png?b=master)](https://scrutinizer-ci.com/g/milvus-io/milvus/)

[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg?style=flat-square)](http://makeapullrequest.com)

# 欢迎来到 Milvus

## Milvus 是什么


Milvus 是一款开源的特征向量相似性搜索引擎。基于异构众核计算框架设计，成本更低，性能更好。在有限的计算资源下，十亿向量搜索仅毫秒响应。

若要了解 Milvus 详细介绍和整体架构，请访问 [Milvus 简介](https://www.milvus.io/cn/docs/about_milvus/overview.md)。

通过 [版本发布说明](https://www.milvus.io/cn/docs/releases/release_notes.md) 获取最新版本的功能和更新。

## 路线图

您可以参考我们的[路线图](https://github.com/milvus-io/milvus/milestones)，了解 Milvus 即将实现的新特性。

路线图尚未完成，并且可能会存在合理改动。我们欢迎各种针对路线图的意见、需求和建议。

## 应用场景

Milvus 可以广泛应用于各类场景。下图展示了基于 Milvus 和 VGG 构建的以图搜图系统。

[![image retrieval demo](https://raw.githubusercontent.com/milvus-io/docs/v0.7.0/assets/image_retrieval.png)](https://raw.githubusercontent.com/milvus-io/docs/v0.7.0/assets/image_retrieval.png)

您可以访问我们的[训练营](https://github.com/milvus-io/bootcamp)探索更多解决方案和应用场景。

## 测试报告

关于 Milvus 性能基准的更多信息，请参考[测试报告](https://github.com/milvus-io/milvus/tree/master/docs)。

## 支持的客户端

-   [Go](https://github.com/milvus-io/milvus-sdk-go)
-   [Python](https://github.com/milvus-io/pymilvus)
-   [Java](https://github.com/milvus-io/milvus-sdk-java)
-   [C++](https://github.com/milvus-io/milvus/tree/master/sdk)
-   [RESTful API](https://github.com/milvus-io/milvus/tree/master/core/src/server/web_impl)
-   [Node.js](https://www.npmjs.com/package/@arkie-ai/milvus-client) (由 [arkie](https://www.arkie.cn/) 提供)

## 开始使用 Milvus

请参阅 [Milvus 安装指南](https://www.milvus.io/cn/docs/guides/get_started/install_milvus/install_milvus.md) 使用 Docker 容器安装 Milvus。若要基于源码编译，请访问 [源码安装](INSTALL.md)。

若要更改 Milvus 设置，请参阅 [Milvus 配置](https://www.milvus.io/cn/docs/reference/milvus_config.md)。

### 开始您的第一个 Milvus 程序

您可以尝试用 [Python](https://www.milvus.io/cn/docs/guides/get_started/example_code.md)，[Java](https://github.com/milvus-io/milvus-sdk-java/tree/master/examples)，[Go](https://github.com/milvus-io/milvus-sdk-go/tree/master/examples)，或者 [C++](https://github.com/milvus-io/milvus/tree/master/sdk/examples) 运行 Milvus 示例代码。


## 贡献者指南

我们由衷欢迎您推送贡献。关于贡献流程的详细信息，请参阅[贡献者指南](https://github.com/milvus-io/milvus/blob/master/CONTRIBUTING.md)。本项目遵循 Milvus [行为准则](https://github.com/milvus-io/milvus/blob/master/CODE_OF_CONDUCT.md)。如果您希望参与本项目，请遵守该准则的内容。

我们使用 [GitHub issues](https://github.com/milvus-io/milvus/issues) 追踪问题和补丁。若您希望提出问题或进行讨论，请加入我们的社区。

## 加入 Milvus 社区

欢迎加入我们的 [Slack 频道](https://join.slack.com/t/milvusio/shared_invite/enQtNzY1OTQ0NDI3NjMzLWNmYmM1NmNjOTQ5MGI5NDhhYmRhMGU5M2NhNzhhMDMzY2MzNDdlYjM5ODQ5MmE3ODFlYzU3YjJkNmVlNDQ2ZTk)以便与其他用户和贡献者进行交流。


## 相关链接

-   [Milvus.io](https://www.milvus.io)

-   [Milvus 在线训练营](https://github.com/milvus-io/bootcamp)

-   [Milvus 测试报告](https://github.com/milvus-io/milvus/tree/master/docs)

-   [Milvus 常见问题](https://www.milvus.io/cn/docs/faq/operational_faq.md)

-   [Milvus Medium](https://medium.com/@milvusio)

-   [Milvus CSDN](https://zilliz.blog.csdn.net/)

-   [Milvus Twitter](https://twitter.com/milvusio)

-   [Milvus Facebook](https://www.facebook.com/io.milvus.5)

-   [Milvus 设计文档](DESIGN.md)

## 许可协议

[Apache 许可协议2.0版](https://github.com/milvus-io/milvus/blob/master/LICENSE)
