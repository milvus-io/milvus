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
[![CodeFactor Grade](https://www.codefactor.io/repository/github/milvus-io/milvus/badge)](https://www.codefactor.io/repository/github/milvus-io/milvus)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/c4bb2ccfb51b47f99e43bfd1705edd95)](https://app.codacy.com/gh/milvus-io/milvus?utm_source=github.com&utm_medium=referral&utm_content=milvus-io/milvus&utm_campaign=Badge_Grade_Dashboard)
[![Scrutinizer Quality Score](https://scrutinizer-ci.com/g/milvus-io/milvus/badges/quality-score.png?b=master)](https://scrutinizer-ci.com/g/milvus-io/milvus/)

[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg?style=flat-square)](http://makeapullrequest.com)

[中文版](README_CN.md) | [日本語版](README_JP.md)

## What is Milvus

Milvus is an open source vector similarity search engine. Built with heterogeneous computing architecture for the best cost efficiency. Searches over billion-scale vectors take only milliseconds with minimum computing resources.

For more detailed introduction of Milvus and its architecture, see [Milvus overview](https://www.milvus.io/docs/about_milvus/overview.md).

Keep up-to-date with newest releases and latest updates by reading Milvus [release notes](https://www.milvus.io/docs/releases/release_notes.md).

## Roadmap

To learn what's coming up soon in Milvus, read our [Roadmap](https://github.com/milvus-io/milvus/milestones).

It is a Work in Progress, and is subject to reasonable adjustments when necessary. And we greatly welcome any comments/requirements/suggestions regarding Milvus roadmap.:clap:

## Application scenarios

Milvus is broadly applicable to a variety of areas. Below screenshot showcases our content-based image retrieval demo system built based on Milvus and VGG.

[![image retrieval demo](https://raw.githubusercontent.com/milvus-io/docs/v0.7.0/assets/image_retrieval.png)](https://raw.githubusercontent.com/milvus-io/docs/v0.7.0/assets/image_retrieval.png)

To explore more Milvus solutions and application scenarios, see our [bootcamp](https://github.com/milvus-io/bootcamp) repository.

## Test reports

See our [test reports](https://github.com/milvus-io/milvus/tree/master/docs) for more information about performance benchmarking of different indexes in Milvus.

## Supported clients

-   [Go](https://github.com/milvus-io/milvus-sdk-go)
-   [Python](https://github.com/milvus-io/pymilvus)
-   [Java](https://github.com/milvus-io/milvus-sdk-java)
-   [C++](https://github.com/milvus-io/milvus/tree/master/sdk)
-   [RESTful API](https://github.com/milvus-io/milvus/tree/master/core/src/server/web_impl)
-   [Node.js](https://www.npmjs.com/package/@arkie-ai/milvus-client) (Provided by [arkie](https://www.arkie.cn/))

## Get started

See the [Milvus install guide](https://www.milvus.io/docs/guides/get_started/install_milvus/install_milvus.md) for using Docker containers. To install Milvus from source code, see [build from source](INSTALL.md).

To edit Milvus settings, read [Milvus configuration](https://www.milvus.io/docs/reference/milvus_config.md).

### Try your first Milvus program

Try running a program with Milvus using [Python](https://www.milvus.io/docs/guides/get_started/example_code.md), [Java](https://github.com/milvus-io/milvus-sdk-java/tree/master/examples), [Go](https://github.com/milvus-io/milvus-sdk-go/tree/master/examples), or [C++ example code](https://github.com/milvus-io/milvus/tree/master/sdk/examples).


## Contribution guidelines

Contributions are welcomed and greatly appreciated. Please read our [contribution guidelines](CONTRIBUTING.md) for detailed contribution workflow. This project adheres to the [code of conduct](CODE_OF_CONDUCT.md) of Milvus. By participating, you are expected to uphold this code.

We use [GitHub issues](https://github.com/milvus-io/milvus/issues) to track issues and bugs. For general questions and public discussions, please join our community.

## Join our community

:heart:To connect with other users and contributors, welcome to join our [Slack channel](https://join.slack.com/t/milvusio/shared_invite/enQtNzY1OTQ0NDI3NjMzLWNmYmM1NmNjOTQ5MGI5NDhhYmRhMGU5M2NhNzhhMDMzY2MzNDdlYjM5ODQ5MmE3ODFlYzU3YjJkNmVlNDQ2ZTk).

See our [community](https://github.com/milvus-io/community) repository to learn about our governance and access more community resources.

## Mailing lists

-   [Milvus TSC](https://lists.lfai.foundation/g/milvus-tsc)
-   [Milvus Technical Discuss](https://lists.lfai.foundation/g/milvus-technical-discuss)
-   [Milvus Announce](https://lists.lfai.foundation/g/milvus-announce)

## Contributors

Below is a list of Milvus contributors. We greatly appreciate your contributions!
<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tr>
    <td align="center"><a href="https://github.com/zerowe-seven"><img src="https://avatars0.githubusercontent.com/u/57790060?v=4" width="100px;" alt=""/><br /><sub><b>zerowe-seven</b></sub></a><br /><a href="https://github.com/milvus-io/milvus/commits?author=zerowe-seven" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/erdustiggen"><img src="https://avatars1.githubusercontent.com/u/25433850?v=4" width="100px;" alt=""/><br /><sub><b>erdustiggen</b></sub></a><br /><a href="https://github.com/milvus-io/milvus/commits?author=erdustiggen" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/gaolizhou"><img src="https://avatars2.githubusercontent.com/u/2884044?v=4" width="100px;" alt=""/><br /><sub><b>gaolizhou</b></sub></a><br /><a href="https://github.com/milvus-io/milvus/commits?author=gaolizhou" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/akihoni"><img src="https://avatars0.githubusercontent.com/u/36330442?v=4" width="100px;" alt=""/><br /><sub><b>Sijie Zhang</b></sub></a><br /><a href="https://github.com/milvus-io/milvus/commits?author=akihoni" title="Documentation">📖</a></td>
    <td align="center"><a href="https://github.com/PizzaL"><img src="https://avatars0.githubusercontent.com/u/5666666?v=4" width="100px;" alt=""/><br /><sub><b>PizzaL</b></sub></a><br /><a href="https://github.com/milvus-io/milvus/commits?author=PizzaL" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/levylll"><img src="https://avatars2.githubusercontent.com/u/5645285?v=4" width="100px;" alt=""/><br /><sub><b>levylll</b></sub></a><br /><a href="https://github.com/milvus-io/milvus/commits?author=levylll" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/aaronjin2010"><img src="https://avatars1.githubusercontent.com/u/48044391?v=4" width="100px;" alt=""/><br /><sub><b>aaronjin2010</b></sub></a><br /><a href="https://github.com/milvus-io/milvus/commits?author=aaronjin2010" title="Code">💻</a></td>
  </tr>
</table>

<!-- markdownlint-enable -->
<!-- prettier-ignore-end -->
<!-- ALL-CONTRIBUTORS-LIST:END -->


## Resources

-   [Milvus.io](https://www.milvus.io)

-   [Milvus FAQ](https://www.milvus.io/docs/faq/operational_faq.md)

-   [Milvus Medium](https://medium.com/@milvusio)

-   [Milvus CSDN](https://zilliz.blog.csdn.net/)

-   [Milvus Twitter](https://twitter.com/milvusio)

-   [Milvus Facebook](https://www.facebook.com/io.milvus.5)

-   [Milvus design docs](DESIGN.md)

## License

[Apache License 2.0](LICENSE)
