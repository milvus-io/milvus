<img src="https://repository-images.githubusercontent.com/208728772/998c09ca-cfa6-4c01-ac75-3dfad7f4862b" alt="milvus banner">

<div class="column" align="middle">
  <a href="https://discord.com/invite/8uyFbECzPX"><img height="20" src="https://img.shields.io/badge/Discord-%235865F2.svg?style=for-the-badge&logo=discord&logoColor=white" alt="license"/></a>
  <img src="https://img.shields.io/github/license/milvus-io/milvus" alt="license"/>
  <img src="https://img.shields.io/docker/pulls/milvusdb/milvus" alt="docker-pull-count" />
</div>

## What is Milvus?

<img src="https://github.com/milvus-io/artwork/blob/master/horizontal/color/milvus-horizontal-color.png" alt="milvus-logo"/>

Milvus is an open-source vector database built to power embedding similarity search and AI applications. Milvus makes unstructured data search more accessible, and provides a consistent user experience regardless of the deployment environment.

Milvus 2.0 is a cloud-native vector database with storage and computation separated by design. All components in this refactored version of Milvus are stateless to enhance elasticity and flexibility. For more architecture details, see [Milvus Architecture Overview](https://milvus.io/docs/architecture_overview.md).

Milvus was released under the [open-source Apache License 2.0](https://github.com/milvus-io/milvus/blob/master/LICENSE) in October 2019. It is currently a graduate project under [LF AI & Data Foundation](https://lfaidata.foundation/).

## Key features

<details>
  <summary><b>Millisecond search on trillion vector datasets</b></summary>
  Average latency measured in milliseconds on trillion vector datasets.
  </details>

<details>
  <summary><b>Simplified unstructured data management</b></summary>
  <li>Rich APIs designed for data science workflows.</li><li>Consistent user experience across laptop, local cluster, and cloud.</li><li>Embed real-time search and analytics into virtually any application.</li>
  </details>

<details>
  <summary><b>Reliable, always on vector database</b></summary>
  Milvus’ built-in replication and failover/failback features ensure data and applications can maintain business continuity in the event of a disruption.
  </details>

<details>
  <summary><b>Highly scalable and elastic</b></summary>
  Component-level scalability makes it possible to scale up and down on demand. Milvus can autoscale at a component level according to the load type, making resource scheduling much more efficient.
  </details>

<details>
  <summary><b>Hybrid search</b></summary>
  In addition to vectors, Milvus supports data types such as Boolean, integers, floating-point numbers, and more. A collection in Milvus can hold multiple fields for accommodating different data features or properties. Milvus pairs scalar filtering with powerful vector similarity search to offer a modern, flexible platform for analyzing unstructured data. Check https://github.com/milvus-io/milvus/wiki/Hybrid-Search for examples and boolean expression rules.
  </details>

<details>
  <summary><b>Unified Lambda structure</b></summary>
  Milvus combines stream and batch processing for data storage to balance timeliness and efficiency. Its unified interface makes vector similarity search a breeze.
  </details>

<details>
  <summary><b>Community supported, industry recognized</b></summary>
  With over 1,000 enterprise users, 9,000+ stars on GitHub, and an active open-source community, you’re not alone when you use Milvus. As a graduate project under the <a href="https://lfaidata.foundation/">LF AI & Data Foundation</a>, Milvus has institutional support.
  </details>

## Quick start

### Start with Zilliz Cloud
Zilliz Cloud is a fully managed service on cloud and the simplest way to deploy LF AI Milvus®, See [Zilliz Cloud Quick Start Guide](https://zilliz.com/doc/quick_start) and start your [free trial](https://cloud.zilliz.com/signup). 

### Install Milvus

- [Standalone Quick Start Guide](https://milvus.io/docs/v2.0.x/install_standalone-docker.md)

- [Cluster Quick Start Guide](https://milvus.io/docs/v2.0.x/install_cluster-docker.md)

- [Advanced Deployment](https://github.com/milvus-io/milvus/wiki)

### Build Milvus from source code

Check the requirements first.

Linux systems (Ubuntu 20.04 or later recommended):
```bash
go: >= 1.20
cmake: >= 3.18
gcc: 7.5
```

MacOS systems with x86_64 (Big Sur 11.5 or later recommended):
```bash
go: >= 1.20
cmake: >= 3.18
llvm: >= 15
```

MacOS systems with Apple Silicon (Monterey 12.0.1 or later recommended):
```bash
go: >= 1.20 (Arch=ARM64)
cmake: >= 3.18
llvm: >= 15
```

Clone Milvus repo and build.

```bash
# Clone github repository.
$ git clone https://github.com/milvus-io/milvus.git

# Install third-party dependencies.
$ cd milvus/
$ ./scripts/install_deps.sh

# Compile Milvus.
$ make
```

For the full story, see [developer's documentation](https://github.com/milvus-io/milvus/blob/master/DEVELOPMENT.md).

> **IMPORTANT** The master branch is for the development of Milvus v2.0. On March 9th, 2021, we released Milvus v1.0, the first stable version of Milvus with long-term support. To use Milvus v1.0, switch to [branch 1.0](https://github.com/milvus-io/milvus/tree/1.0).

## Milvus 2.0 vs. 1.x: Cloud-native, distributed architecture, highly scalable, and more

See [Milvus 2.0 vs. 1.x](https://github.com/milvus-io/milvus/blob/master/milvus20vs1x.md) for more information.

## Real world demos

<table>
  <tr>
    <td width="30%">
      <a href="https://milvus.io/milvus-demos">
        <img src="https://assets.zilliz.com/image_search_59a64e4f22.gif" />
      </a>
    </td>
    <td width="30%">
<a href="https://milvus.io/milvus-demos">
<img src="https://assets.zilliz.com/qa_df5ee7bd83.gif" />
</a>
    </td>
    <td width="30%">
<a href="https://milvus.io/milvus-demos">
<img src="https://assets.zilliz.com/mole_search_76f8340572.gif" />
</a>
    </td>
  </tr>
  <tr>
    <th>
      <a href="https://milvus.io/milvus-demos">Image search</a>
    </th>
    <th>
      <a href="https://milvus.io/milvus-demos">Chatbots</a>
    </th>
    <th>
      <a href="https://milvus.io/milvus-demos">Chemical structure search</a>
    </th>
  </tr>
</table>

#### Image Search

Images made searchable. Instantaneously return the most similar images from a massive database.

#### Chatbots

Interactive digital customer service that saves users time and businesses money.

#### Chemical Structure Search

Blazing fast similarity search, substructure search, or superstructure search for a specified molecule.

## Bootcamps

Milvus [bootcamp](https://github.com/milvus-io/bootcamp) is designed to expose users to both the simplicity and depth of the vector database. Discover how to run benchmark tests as well as build similarity search applications spanning chatbots, recommendation systems, reverse image search, molecular search, and much more.

## Contributing

Contributions to Milvus are welcome from everyone. See [Guidelines for Contributing](https://github.com/milvus-io/milvus/blob/master/CONTRIBUTING.md) for details on submitting patches and the contribution workflow. See our [community repository](https://github.com/milvus-io/community) to learn about our governance and access more community resources.

### All contributors