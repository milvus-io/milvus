<center>
<img src="https://zillizstorage.blob.core.windows.net/zilliz-assets/zilliz-assets/assets/small_v2_0_readme_33ea8d0e66.jpg" alt="milvus banner"width="960" height="540">
</center>





<center>
  <a href="https://join.slack.com/t/milvusio/shared_invite/zt-e0u4qu3k-bI2GDNys3ZqX1YCJ9OM~GQ">
        <img src="https://img.shields.io/badge/Join-Slack-orange" />
  </a>
        <img src="https://img.shields.io/github/license/milvus-io/milvus" />
        <img src="https://img.shields.io/docker/pulls/milvusdb/milvus" />
</center>


<center>
  <a href="http://internal.zilliz.com:18080/jenkins/job/milvus-ci/job/master/">
        <img src="http://internal.zilliz.com:18080/jenkins/job/milvus-ci/job/master/badge/icon" />
  </a>
  <a href="https://bestpractices.coreinfrastructure.org/projects/3563">
        <img src="https://bestpractices.coreinfrastructure.org/projects/3563/badge" />
  </a>
  <a href="https://codecov.io/gh/milvus-io/milvus">
        <img src="https://codecov.io/gh/milvus-io/milvus/branch/master/graph/badge.svg" />
  </a>
  <a href="https://app.codacy.com/gh/milvus-io/milvus?utm_source=github.com&utm_medium=referral&utm_content=milvus-io/milvus&utm_campaign=Badge_Grade_Dashboard">
        <img src="https://api.codacy.com/project/badge/Grade/c4bb2ccfb51b47f99e43bfd1705edd95" />
  </a>
</center>




<details>
<summary>Take a quick look at our demos!</summary>
  <table>
  <tr>
    <td width="30%">
      <a href="https://zilliz.com/solutions">
        <img src="https://zillizstorage.blob.core.windows.net/zilliz-assets/zilliz-assets/assets/image_search_59a64e4f22.gif" />
      </a>
    </td>
    <td width="30%">
<a href="https://zilliz.com/solutions">
<img src="https://zillizstorage.blob.core.windows.net/zilliz-assets/zilliz-assets/assets/qa_df5ee7bd83.gif" />
</a>
    </td>
    <td width="30%">
<a href="https://zilliz.com/solutions">
<img src="https://zillizstorage.blob.core.windows.net/zilliz-assets/zilliz-assets/assets/mole_search_76f8340572.gif" />
</a>
    </td>
  </tr>
  <tr>
    <th>
      <a href="https://zilliz.com/solutions">Image search</a>
    </th>
    <th>
      <a href="https://zilliz.com/solutions">Chatbots</a>
    </th>
    <th>
      <a href="https://zilliz.com/solutions">Chemical structure search</a>
    </th>
  </tr>
</table>
</details>


Milvus is an AI-infused database geared towards (embedding) vector similarity search. Milvus is dedicated to lowering the bar for unstructured data search and providing a consistent user experience regardless of users' deployment environment. 

Milvus was released under the open-source Apache License 2.0 in October 2019. It is currently an incubation-stage project under [LF AI & Data Foundation](https://lfaidata.foundation/). 

- **Functionality-level Autoscaling**

With the main functionalities implemented equivalent among nodes, Milvus is able to autoscale at the functionality level, providing the foundation for a more efficient resource scheduling. 

- **Hybrid Search**

In addition to vectors, basic numeric types, such as boolean, integer, floating-point number, etc, are introduced in Milvus. Search for data from hybrid fields are now supported in the Milvus collection.

- **Combined Data Storage**

Milvus has reinforced its support for both streaming and batch data persistence and for the adaptation of alternative message/storage engines, in response to users' increasing demand for high database throughput.

- **Multiple Indexes in a Single Field**

Milvus now supports multiple indexes in a single vector filed, and it decouples indexing from querying. Users are allowed to maintain multiple indexes simultaneously and switch flexibly among them according to their needs.

> **IMPORTANT** The master branch is for the development of Milvus v2.0. On March 9th, 2021, we released Milvus v1.0 which is our first stable version of Milvus with long-term support. To try out Milvus v1.0, switch to [branch 1.0](https://github.com/milvus-io/milvus/tree/1.0).

## Getting Started

### To install a Milvus stand-alone

See [Install Milvus Standalone]().

### To install a Milvus cluster

See [Install Milvus Cluster]().

### Demos

- [Image Search](https://zilliz.com/milvus-demos): Images made searchable. Instantaneously return the most similar images from a massive database.
- [Chatbots](https://zilliz.com/milvus-demos): Interactive digital customer service that saves users time and businesses money.
- [Chemical Structure Search](https://zilliz.com/milvus-demos): Blazing fast similarity search, substructure search, or superstructure search for a specified molecule.

## Contributing

Contributions to Milvus are welcome from everyone. See [Guidelines for Contributing](https://github.com/milvus-io/milvus/blob/master/CONTRIBUTING.md) for details on submitting patches and the contribution workflow. See our [community repository](https://github.com/milvus-io/community) to learn about our governance and access more community resources.

## Documentation

### Milvus Docs

For documentation about Milvus, see [Milvus Docs](https://milvus.io/docs/overview.md).

### SDK

The implemented SDK and its API document are listed below:

- [Python](https://github.com/milvus-io/pymilvus/tree/1.x)

### Recommended Articles

- [What is an embedding vector? Why and how does it contribute to the development of Machine Learning?](https://milvus.io/docs/v1.0.0/vector.md)
- [What types of vector index does Milvus support? Which should I choose?](https://milvus.io/docs/v1.0.0/index.md)
- [How does Milvus compare the distance between vectors?](https://milvus.io/docs/v1.0.0/metric.md)
- You can learn more in [Milvus Server Configurations](https://milvus.io/docs/v1.0.0/milvus_config.md).

## Contact

Join the Milvus community on [Slack Channel](https://join.slack.com/t/milvusio/shared_invite/zt-e0u4qu3k-bI2GDNys3ZqX1YCJ9OM~GQ) to share your suggestions, advice, and questions with our engineer team. You can also ask for help at our [FAQ page](https://milvus.io/docs/v1.0.0/performance_faq.md).

You can subscribe to our mailing lists at:

- [Milvus Technical Steering Committee](https://lists.lfai.foundation/g/milvus-tsc)
- [Milvus Technical Discussions](https://lists.lfai.foundation/g/milvus-technical-discuss)
- [Milvus Announcement](https://lists.lfai.foundation/g/milvus-announce)

and follow us on social media:

- [Milvus Medium](https://medium.com/@milvusio)
- [Milvus CSDN](https://zilliz.blog.csdn.net/)
- [Milvus Twitter](https://twitter.com/milvusio)
- [Milvus Facebook](https://www.facebook.com/io.milvus.5)

## License

Milvus is licensed under the Apache License, Version 2.0. View a copy of the [License file](https://github.com/milvus-io/milvus/blob/master/LICENSE).

## Acknowledgments

Milvus adopts dependencies from the following:

- Thank [FAISS](https://github.com/facebookresearch/faiss) for the excellent search library.
- Thank [etcd](https://github.com/coreos/etcd) for providing some great open-source tools.
- Thank [Pulsar](https://github.com/apache/pulsar) for its great distributed information pub/sub platform.
- Thank [RocksDB](https://github.com/facebook/rocksdb) for the powerful storage engines.
