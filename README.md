![Milvuslogo](https://github.com/milvus-io/docs/blob/master/v0.9.1/assets/milvus_logo.png)
[![Slack](https://img.shields.io/badge/Join-Slack-orange)](https://join.slack.com/t/milvusio/shared_invite/zt-e0u4qu3k-bI2GDNys3ZqX1YCJ9OM~GQ)
![GitHub](https://img.shields.io/github/license/milvus-io/milvus)
![Docker pulls](https://img.shields.io/docker/pulls/milvusdb/milvus)

[![Build Status](http://internal.zilliz.com:18080/jenkins/job/milvus-ci/job/master/badge/icon)](http://internal.zilliz.com:18080/jenkins/job/milvus-ci/job/master/)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/3563/badge)](https://bestpractices.coreinfrastructure.org/projects/3563)
[![codecov](https://codecov.io/gh/milvus-io/milvus/branch/master/graph/badge.svg)](https://codecov.io/gh/milvus-io/milvus)
[![codebeat badge](https://codebeat.co/badges/e030a4f6-b126-4475-a938-4723d54ec3a7?style=plastic)](https://codebeat.co/projects/github-com-milvus-io-milvus-master)
[![CodeFactor Grade](https://www.codefactor.io/repository/github/milvus-io/milvus/badge)](https://www.codefactor.io/repository/github/milvus-io/milvus)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/c4bb2ccfb51b47f99e43bfd1705edd95)](https://app.codacy.com/gh/milvus-io/milvus?utm_source=github.com&utm_medium=referral&utm_content=milvus-io/milvus&utm_campaign=Badge_Grade_Dashboard)


# Welcome to the world of Milvus

## What is Milvus

Milvus is an embeddings similarity search engine that is highly flexible, reliable, and blazing fast. It supports adding, deleting, updating, and near-real-time search of embeddings on a scale of trillion bytes. By encapsulating multiple widely adopted index libraries, such as Faiss, NMSLIB, and Annoy, it provides a comprehensive set of intuitive APIs, allowing you to choose index types based on your scenario. By supporting the filtering of scalar data, Milvus takes the recall rate even higher and adds more flexibility to your search. 

The Milvus architecture is as follows:

![arch](https://github.com/milvus-io/docs/blob/master/v0.9.1/assets/milvus_arch.png)

For a more detailed introduction of Milvus and its architecture, see [Milvus overview](https://www.milvus.io/docs/overview.md). To keep up-to-date with its releases and updates, see Milvus [release notes](https://www.milvus.io/docs/release_notes.md).

Milvus was released under the Apache 2.0 License and officially open sourced in October 2019. It is an incubation-stage project at [LF AI & Data Foundation](https://lfaidata.foundation/). The source code of Milvus is hosted on [GitHub](https://github.com/milvus-io/milvus).

## Get started

### Install Milvus

- To install Milvus using Docker, see [Milvus install guide](https://www.milvus.io/docs/install_milvus.md) 
- To install Milvus from source code, see [build from source](INSTALL.md).

### Try example programs

You can use [Python](https://www.milvus.io/docs/example_code.md), [Java](https://github.com/milvus-io/milvus-sdk-java/tree/master/examples), [Go](https://github.com/milvus-io/milvus-sdk-go/tree/master/examples), or [C++](https://github.com/milvus-io/milvus/tree/master/sdk/examples) example code to try a Milvus example program.

## Supported clients

-   [Go](https://github.com/milvus-io/milvus-sdk-go)
-   [Python](https://github.com/milvus-io/pymilvus)
-   [Java](https://github.com/milvus-io/milvus-sdk-java)
-   [C++](https://github.com/milvus-io/milvus/tree/master/sdk)
-   [RESTful API](https://github.com/milvus-io/milvus/tree/master/core/src/server/web_impl)
-   [Node.js](https://www.npmjs.com/package/@arkie-ai/milvus-client) (Contributed by [arkie](https://www.arkie.cn/))

## Application scenarios

Milvus has been used in hundreds of organizations and institutions worldwide mainly in the following scenarios:

- Image, video, and audio search.
- Text search, recommender system, interactive question answering system, and other text search fields.
- Drug discovery, genetic screening, and other biomedical fields.

See [Scenarios](https://www.milvus.io/scenarios/) for more detailed application scenarios and demos.

See [Milvus Bootcamp](https://github.com/milvus-io/bootcamp) for detailed solutions and application scenarios.

## Benchmark

See our [test reports](https://github.com/milvus-io/bootcamp/tree/master/EN_benchmark_test) for more information about performance benchmarking of different indexes in Milvus.

## Roadmap

To learn what's coming up soon in Milvus, read our [Roadmap](https://github.com/milvus-io/milvus/milestones).

It is a **Work in Progress**, and is subject to reasonable adjustments when necessary. And we greatly appreciate any comments, requirements and suggestions on the Milvus roadmap.:clap:

## Join our community

You are welcome to join our community. :heart: We appreciate any contributions from you. 

- For a detailed contribution workflow, see our [contribution guidelines](CONTRIBUTING.md). 
- All the contributors should follow the [code of conduct](CODE_OF_CONDUCT.md) of Milvus.
- To track issues and bugs, use [GitHub issues](https://github.com/milvus-io/milvus/issues).
- To connect with other users and contributors, welcome to join our [Slack channel](https://join.slack.com/t/milvusio/shared_invite/zt-e0u4qu3k-bI2GDNys3ZqX1YCJ9OM~GQ).

See our [community](https://github.com/milvus-io/community) repository to learn more about our governance and access more community resources.

## Join Milvus WeChat group

![qrcode](https://github.com/milvus-io/docs/blob/master/v0.11.0/assets/qrcode.png)

## Resources 

- [Milvus.io](https://www.milvus.io) 

- [Milvus FAQ](https://www.milvus.io/docs/operational_faq.md) 

- [Milvus Medium](https://medium.com/@milvusio) 

- [Milvus CSDN](https://zilliz.blog.csdn.net/) 

- [Milvus Twitter](https://twitter.com/milvusio) 

- [Milvus Facebook](https://www.facebook.com/io.milvus.5) 

- [Milvus design docs](DESIGN.md) 

## License 

[Apache License 2.0](LICENSE)
