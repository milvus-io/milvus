![Milvuslogo](https://raw.githubusercontent.com/milvus-io/docs/master/assets/milvus_logo.png)

[![Slack](https://img.shields.io/badge/Join-Slack-orange)](https://join.slack.com/t/milvusio/shared_invite/enQtNzY1OTQ0NDI3NjMzLWNmYmM1NmNjOTQ5MGI5NDhhYmRhMGU5M2NhNzhhMDMzY2MzNDdlYjM5ODQ5MmE3ODFlYzU3YjJkNmVlNDQ2ZTk)
![LICENSE](https://img.shields.io/badge/license-Apache--2.0-brightgreen)
![Language](https://img.shields.io/badge/language-C%2B%2B-blue)
[![codebeat badge](https://codebeat.co/badges/e030a4f6-b126-4475-a938-4723d54ec3a7?style=plastic)](https://codebeat.co/projects/github-com-jinhai-cn-milvus-master)
![Release](https://img.shields.io/badge/release-v0.5.3-yellowgreen)
![Release_date](https://img.shields.io/badge/release_date-October-yellowgreen)
[![codecov](https://codecov.io/gh/milvus-io/milvus/branch/master/graph/badge.svg)](https://codecov.io/gh/milvus-io/milvus)


# 欢迎来到 Milvus

## Milvus 是什么

Milvus 是一款开源的、针对海量特征向量的相似性搜索引擎。基于异构众核计算框架设计，成本更低，性能更好。在有限的计算资源下，十亿向量搜索仅毫秒响应。

若要了解 Milvus 详细介绍和整体架构，请访问 [Milvus 简介](https://www.milvus.io/docs/zh-CN/aboutmilvus/overview/)。

Milvus 提供稳定的 [Python](https://github.com/milvus-io/pymilvus)、[Java](https://github.com/milvus-io/milvus-sdk-java) 以及[C++](https://github.com/milvus-io/milvus/tree/master/core/src/sdk) 的 API 接口。

通过 [版本发布说明](https://milvus.io/docs/zh-CN/release/v0.5.3/) 获取最新版本的功能和更新。

## 开始使用 Milvus

请参阅 [Milvus 安装指南](https://www.milvus.io/docs/zh-CN/userguide/install_milvus/) 使用 Docker 容器安装 Milvus。若要基于源码编译，请访问 [源码安装](install.md)。

若要更改 Milvus 设置，请参阅 [Milvus 配置](https://www.milvus.io/docs/zh-CN/reference/milvus_config/)。

### 开始您的第一个 Milvus 程序

您可以尝试用 [Python](https://www.milvus.io/docs/en/userguide/example_code/) 或 [Java example code](https://github.com/milvus-io/milvus-sdk-java/tree/master/examples) 运行 Milvus 示例代码。

若要使用 C++ 示例代码，请使用以下命令：

```shell
 # Run Milvus C++ example
 $ cd [Milvus root path]/core/milvus/bin
 $ ./sdk_simple
```

## 路线图

请阅读我们的[路线图](https://milvus.io/docs/zh-CN/roadmap/)以了解更多即将开发的新功能。

## 贡献者指南

我们由衷欢迎您推送贡献。关于贡献流程的详细信息，请参阅[贡献者指南](https://github.com/milvus-io/milvus/blob/master/CONTRIBUTING.md)。本项目遵循 Milvus [行为准则](https://github.com/milvus-io/milvus/blob/master/CODE_OF_CONDUCT.md)。如果您希望参与本项目，请遵守该准则的内容。

我们使用 [GitHub issues](https://github.com/milvus-io/milvus/issues) 追踪问题和补丁。若您希望提出问题或进行讨论，请加入我们的社区。

## 加入 Milvus 社区

欢迎加入我们的 [Slack 频道](https://join.slack.com/t/milvusio/shared_invite/enQtNzY1OTQ0NDI3NjMzLWNmYmM1NmNjOTQ5MGI5NDhhYmRhMGU5M2NhNzhhMDMzY2MzNDdlYjM5ODQ5MmE3ODFlYzU3YjJkNmVlNDQ2ZTk)以便与其他用户和贡献者进行交流。

## 贡献者

以下是 Milvus 贡献者名单，在此我们深表感谢：

- [akihoni](https://github.com/akihoni) 提供了中文版 README，并发现了 README 中的无效链接。
- [goodhamgupta](https://github.com/goodhamgupta) 发现并修正了在线训练营文档中的文件名拼写错误。
- [erdustiggen](https://github.com/erdustiggen) 将错误信息里的 std::cout 修改为 LOG，修正了一个 Clang 格式问题和一些语法错误。

## 相关链接

- [Milvus.io](https://www.milvus.io)

- [Milvus 在线训练营](https://github.com/milvus-io/bootcamp)

- [Milvus 测试报告](https://github.com/milvus-io/milvus/tree/master/docs)

- [常见问题](https://www.milvus.io/docs/zh-CN/faq/operational_faq/)

- [Milvus Medium](https://medium.com/@milvusio)

- [Milvus CSDN](https://zilliz.blog.csdn.net/)

- [Milvus Twitter](https://twitter.com/milvusio)

- [Milvus Facebook](https://www.facebook.com/io.milvus.5)

## 许可协议

[Apache 许可协议2.0版](https://github.com/milvus-io/milvus/blob/master/LICENSE)
