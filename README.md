<img src="https://github.com/user-attachments/assets/51e33300-7f85-43ff-a05a-3a0317a961f3" alt="milvus banner">

<div class="column" align="middle">
  <a href="https://github.com/milvus-io/milvus/blob/master/LICENSE"><img height="20" src="https://img.shields.io/github/license/milvus-io/milvus" alt="license"/></a>
  <a href="https://milvus.io/docs/install_standalone-docker.md"><img src="https://img.shields.io/docker/pulls/milvusdb/milvus" alt="docker-pull-count"/></a>
  <a href="https://discord.com/invite/8uyFbECzPX"><img height="20" src="https://img.shields.io/badge/Discord-%235865F2.svg?style=for-the-badge&logo=discord&logoColor=white" alt="discord"/></a>
</div>

## What is Milvus?

[Milvus](https://milvus.io/) is a high-performance vector database built for scale. It is used by AI applications to organize and search through large amount of unstructured data, such as text, images, and multi-modal information.

Milvus is implemented with Go and C++ and employs CPU/GPU instruction-level optimization for best vector search performance. With [fully-distributed architecture on K8s](https://milvus.io/docs/overview.md#What-Makes-Milvus-so-Scalable), it can handle tens of thousands of search queries on billions of vectors, scale horizontally and maintain data freshness by processing streaming updates in real-time. For smaller use cases, Milvus supports [Standalone mode](https://milvus.io/docs/install_standalone-docker.md) that can run on Docker. In addition, [Milvus Lite](https://milvus.io/docs/milvus_lite.md) is a lightweight version suitable for quickstart in python, with simply `pip install`.

The easiest way to try out Milvus is to use [Zilliz Cloud with free trial](https://cloud.zilliz.com/signup). Milvus is available as a fully managed service on Zilliz Cloud.

The Milvus open-source project is
under [LF AI & Data Foundation](https://lfaidata.foundation/projects/milvus/), distributed with [Apache 2.0](https://github.com/milvus-io/milvus/blob/master/LICENSE) License.

## Quickstart

```python
$ pip install -U pymilvus
```
This installs `pymilvus`, the Python SDK for Milvus. Use `MilvusClient` to create a client:
```python
from pymilvus import MilvusClient
```

* `pymilvus` also includes Milvus Lite for quickstart. To create a local vector database, simply instantiate a client with a local file name for persisting data:

  ```python
  client = MilvusClient("milvus_demo.db")
  ```

* You can also specify the credentials to connect to your deployed [Milvus server](https://milvus.io/docs/authenticate.md?tab=docker) or [Zilliz Cloud](https://docs.zilliz.com/docs/quick-start):

  ```python
  client = MilvusClient(
    uri="<endpoint_of_self_hosted_milvus_or_zilliz_cloud>",
    token="<username_and_password_or_zilliz_cloud_api_key>")
  ```

With the client, you can create collection:
```python
client.create_collection(
    collection_name="demo_collection",
    dimension=768,  # The vectors we will use in this demo has 768 dimensions
)
```

Ingest data:
```python
res = client.insert(collection_name="demo_collection", data=data)
```

Perform vector search:

```python
query_vectors = embedding_fn.encode_queries(["Who is Alan Turing?", "What is AI?"])
res = client.search(
    collection_name="demo_collection",  # target collection
    data=query_vectors,  # a list of one or more query vectors, supports batch
    limit=2,  # how many results to return (topK)
    output_fields=["vector", "text", "subject"],  # what fields to return
)
```

## Why Milvus

Milvus is designed to handle vector search at scale. Users can store vectors, which are numerical representations of unstructured data, together with other scalar data types such as integers, strings, and JSON objects, to conduct efficient vector search with metadata filtering or hybrid search. Here are why users choose Milvus as vector database:

**High Performance at Scale and High Availability**  
  * Milvus features a [distributed architecture](https://milvus.io/docs/architecture_overview.md ) that separates [compute](https://milvus.io/docs/data_processing.md#Data-query) and [storage](https://milvus.io/docs/data_processing.md#Data-insertion). Milvus can horizontally scale and adapt to diverse traffic patterns, by independent scaling worker nodes for optimized performance in read-heavy or write-heavy workloads. The stateless microservices on K8s allow [quick recovery](https://milvus.io/docs/coordinator_ha.md#Coordinator-HA) from failure, ensuring high availability. [Replication](https://milvus.io/docs/replica.md) further enhances fault tolerance and throughput by loading data segments on multiple query nodes. Milvus also optimizes vector search at high metadata filtering rate. See [performance benchmark](https://zilliz.com/vector-database-benchmark-tool).


**Support for Various Vector Index Types and Hardware Acceleration**  
  * Milvus supports vector index types that are optimized for different scenarios, including HNSW, IVF, FLAT (brute-force), and DiskANN, with [quantization-based](https://milvus.io/docs/index.md?tab=floating#IVFPQ) variations and [mmap](https://milvus.io/docs/mmap.md). Additionally, Milvus implements instruction-level acceleration to enhance vector search performance and supports GPU indexing, such as NVIDIA's [CAGRA](https://github.com/rapidsai/raft).

**Sparse Vector for Full Text Search and Hybrid Search**
  * In addition to semantic search via dense vector embeddings, Milvus also supports full text search with [sparse vector](https://milvus.io/docs/sparse_vector.md). User can also combine dense vector and sparse vector through multi-vector feature and perform hybrid search. Milvus supports up to 10 vector fields in a single collection. Milvus supports hybrid search on dense and sparse vector columns, or multiple dense vector columns. The data from multi-path retrieval can be merge and rerank with Reciprocal Rank Fusion (RRF) and Weighted Scoring. For details, refer to [Hybrid Search](https://milvus.io/docs/multi-vector-search.md).

**Flexible Strategies for Multi-tenancy**
  * Milvus supports [multi-tenancy](https://milvus.io/docs/multi_tenancy.md#Multi-tenancy-strategies) with flexible strategies to provide a robust solution for organizing data in AI applications, particularly in Retrieval-Augmented Generation (RAG) systems for both enterprise use cases and consumer apps. By leveraging database, collection, and partition key structures, Milvus allows businesses to allocate specific resources to each tenant, enabling flexible data isolation, optimized search performance, and scalable access management. This approach suits large-scale knowledge bases for enterprises, with strong isolation at the database level, and high user capacity for consumer applications by partitioning data within shared collections.

**Intuitive API and SDKs**
  * Milvus implements easy-to-use RESTful and gRPC API for vector search, data query and data management. Milvus also provides SDKs for [Python](https://github.com/milvus-io/pymilvus), [Java](https://github.com/milvus-io/milvus-sdk-java), [Go](https://github.com/milvus-io/milvus-sdk-go), [C++](https://github.com/milvus-io/milvus-sdk-cpp), [Node.js](https://github.com/milvus-io/milvus-sdk-node), [Rust](https://github.com/milvus-io/milvus-sdk-rust), and [C#](https://github.com/milvus-io/milvus-sdk-csharp) languages. In addition, Milvus also provides bulk insert API for large scale batch [data import](https://milvus.io/docs/import-data.md) and [data backup](https://milvus.io/docs/milvus_backup_overview.md).

**Rich Ecosystem and Tools**
  * Milvus integrates with a comprehensive suite of [AI development tools](](https://milvus.io/docs/integrations_overview.md)), such as LangChain, LlamaIndex, Haystack, Ragas, making it an ideal vector store for GenAI applications such as Retrieval-Augmented Generation (RAG). Through the [`pymilvus` library](https://milvus.io/docs/embeddings.md), users can easily transform unstructured data into vector embeddings and leverage reranking models for optimized search results. The Milvus ecosystem also includes [Attu](https://github.com/zilliztech/attu?tab=readme-ov-file#attu) for GUI-based administration, [Birdwatcher](https://milvus.io/docs/birdwatcher_overview.md) for system debugging, [Prometheus/Grafana](https://milvus.io/docs/monitor_overview.md) for monitoring, [Milvus CDC](https://milvus.io/docs/milvus-cdc-overview.md) for data synchronization, and data connectors for [Spark](https://milvus.io/docs/integrate_with_spark.md#Spark-Milvus-Connector-User-Guide), [Kafka](https://github.com/zilliztech/kafka-connect-milvus?tab=readme-ov-file#kafka-connect-milvus-connector), [Fivetran](https://fivetran.com/docs/destinations/milvus), and [Airbyte](https://milvus.io/docs/integrate_with_airbyte.md) to streamline AI and search pipelines.

Milvus is trusted by AI developers to build applications such as text and image search, Retrieval-Augmented Generation (RAG), and recommendation systems. Milvus powers [many mission-critical business]((https://milvus.io/use-cases)) for startups and enterprises.

## Demos and Tutorials 
Here is a selection of demos and tutorials to show how to build various types of AI applications made with Milvus:

| Tutorial | Use Case | Related Milvus Features | 
| -------- | -------- | --------- |
| [Build RAG with Milvus](build-rag-with-milvus.md) |  RAG | vector search |
| [Multimodal RAG with Milvus](multimodal_rag_with_milvus.md) | RAG | vector search, dynamic field |
| [Image Search with Milvus](image_similarity_search.md) | Semantic Search | vector search, dynamic field |
| [Hybrid Search with Milvus](hybrid_search_with_milvus.md) | Hybrid Search | hybrid search, multi vector, dense embedding, sparse embedding |
| [Multimodal Search using Multi Vectors](multimodal_rag_with_milvus.md) | Semantic Search | multi vector, hybrid search |
| [Recommender System](recommendation_system.md) | Recommendation System | vector search |
| [Video Similarity Search](video_similarity_search.md) | Semantic Search | vector search |
| [Audio Similarity Search](audio_similarity_search.md) | Semantic Search | vector search |
| [DNA Classification](dna_sequence_classification.md) | Classification | vector search |
| [Graph RAG with Milvus](graph_rag_with_milvus.md) | RAG | vector search |
| [Contextual Retrieval with Milvus](contextual_retrieval_with_milvus.md) | RAG, Semantic Search | vector search |
| [HDBSCAN Clustering with Milvus](hdbscan_clustering_with_milvus.md) | Clustering | vector search |
| [Use ColPali for Multi-Modal Retrieval with Milvus](use_ColPali_with_milvus.md) | RAG, Semantic Search | vector search |
| [Vector Visualization](vector_visualization.md) | Data Visualization | vector search |

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
      <a href="https://milvus.io/milvus-demos">Image Search</a>
    </th>
    <th>
      <a href="https://milvus.io/milvus-demos">RAG</a>
    </th>
    <th>
      <a href="https://milvus.io/milvus-demos">Drug Discovery</a>
    </th>
  </tr>
</table>

## Integration

Milvus integrates with popular AI development stacks such as LangChain, LlamaIndex, and OpenAI, providing a robust vector database foundation that supports a variety of Retrieval-Augmented Generation (RAG), semantic search, multi-modal and agent-based applications. Milvus works for both open-source embedding models and embedding service, in text, image and video modalities. Milvus has connectors with third-party tools and data sources like Fivetran, Airbyte, Apify, Apache Spark and Apache Kafka that allows developers to create advanced data pipelines. Milvus can also work with tools for orchestrating, evaluating, and optimizing RAG workflows.

Check out https://milvus.io/docs/integrations_overview.md for more details.

## Documentation

For guidance on installation, usage, deployment, and administration, check out [Milvus Docs](https://milvus.io/docs). For technical milestones and enhancement proposals, check out [issues on GitHub](https://github.com/milvus-io/milvus/issues).

## Contributing

The Milvus open-source project accepts contribution from everyone. See [Guidelines for Contributing](https://github.com/milvus-io/milvus/blob/master/CONTRIBUTING.md) for details on submitting patches and the development workflow. See our [community repository](https://github.com/milvus-io/community) to learn about project governance and access more community resources.

### Build Milvus from Source Code

Requirements:

* Linux systems (Ubuntu 20.04 or later recommended):
  ```bash
  go: >= 1.21
  cmake: >= 3.26.4
  gcc: 9.5
  python: > 3.8 and  <= 3.11
  ```

* MacOS systems with x86_64 (Big Sur 11.5 or later recommended):
  ```bash
  go: >= 1.21
  cmake: >= 3.26.4
  llvm: >= 15
  python: > 3.8 and  <= 3.11
  ```

* MacOS systems with Apple Silicon (Monterey 12.0.1 or later recommended):
  ```bash
  go: >= 1.21 (Arch=ARM64)
  cmake: >= 3.26.4
  llvm: >= 15
  python: > 3.8 and  <= 3.11
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

For full instructions, see [developer's documentation](https://github.com/milvus-io/milvus/blob/master/DEVELOPMENT.md).

## Community

Join the Milvus community on [Discord](https://discord.gg/8uyFbECzPX) to share your suggestions, advice, and questions with our engineering team.

You can also check out our [FAQ page](https://milvus.io/docs/performance_faq.md) to discover solutions or answers to your issues or questions.

Subscribe to Milvus mailing lists:

- [Technical Steering Committee](https://lists.lfai.foundation/g/milvus-tsc)
- [Technical Discussions](https://lists.lfai.foundation/g/milvus-technical-discuss)
- [Announcement](https://lists.lfai.foundation/g/milvus-announce)

Follow Milvus on social media:

- [X](https://twitter.com/milvusio)
- [LinkedIn](https://www.linkedin.com/company/the-milvus-project)
- [Youtube](https://www.youtube.com/channel/UCMCo_F7pKjMHBlfyxwOPw-g)
- [Medium](https://medium.com/@milvusio)

## Reference

Reference to cite when you use Milvus in a research paper:

```
@inproceedings{2021milvus,
  title={Milvus: A Purpose-Built Vector Data Management System},
  author={Wang, Jianguo and Yi, Xiaomeng and Guo, Rentong and Jin, Hai and Xu, Peng and Li, Shengjun and Wang, Xiangyu and Guo, Xiangzhou and Li, Chengming and Xu, Xiaohai and others},
  booktitle={Proceedings of the 2021 International Conference on Management of Data},
  pages={2614--2627},
  year={2021}
}

@article{2022manu,
  title={Manu: a cloud native vector database management system},
  author={Guo, Rentong and Luan, Xiaofan and Xiang, Long and Yan, Xiao and Yi, Xiaomeng and Luo, Jigao and Cheng, Qianya and Xu, Weizhi and Luo, Jiarui and Liu, Frank and others},
  journal={Proceedings of the VLDB Endowment},
  volume={15},
  number={12},
  pages={3548--3561},
  year={2022},
  publisher={VLDB Endowment}
}
```
