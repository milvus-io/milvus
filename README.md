<img src="https://zillizstorage.blob.core.windows.net/zilliz-assets/zilliz-assets/assets/readme_en_9f910915cf.png" alt="milvus banner">




<div class="column" align="middle">
<a href="https://join.slack.com/t/milvusio/shared_invite/zt-e0u4qu3k-bI2GDNys3ZqX1YCJ9OM~GQ">
        <img src="https://img.shields.io/badge/Join-Slack-orange" /></a>
        <img src="https://img.shields.io/github/license/milvus-io/milvus" />
        <img src="https://img.shields.io/docker/pulls/milvusdb/milvus" />
</div>



<div class="column" align="middle">
  <a href="https://internal.zilliz.com:18080/jenkins/job/milvus-ha-ci/job/master/badge/">
        <img src="https://internal.zilliz.com:18080/jenkins/job/milvus-ha-ci/job/master/badge/icon" />
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
</div>



<br />

## What is Milvus?

Milvus is an open-source vector database built to power AI applications and embedding similarity search. Milvus makes unstructured data search more accessible, and provides a consistent user experience regardless of the deployment environment.

Both Milvus Standalone and Milvus Cluster are available.


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
  In addition to vectors, Milvus supports data types such as Boolean, integers, floating-point numbers, and more. A collection in Milvus can hold multiple fields for accommodating different data features or properties. By complementing scalar filtering to vector similarity search, Milvus makes modern search much more flexible than ever.
  </details>

<details>
  <summary><b>Unified Lambda structure</b></summary>
  Milvus combines stream and batch processing for data storage to balance timeliness and efficiency. Its unified interface makes vector similarity search a breeze.
  </details>

<details>
  <summary><b>Community supported, industry recognized</b></summary>
  With over 1,000 enterprise users, 6,000+ stars on GitHub, and an active open-source community, you’re not alone when you use Milvus. As a graduate project under the <a href="https://lfaidata.foundation/">LF AI & Data Foundation</a>, Milvus has institutional support.
  </details>


> **IMPORTANT** The master branch is for the development of Milvus v2.0. On March 9th, 2021, we released Milvus v1.0, the first stable version of Milvus with long-term support. To use Milvus v1.0, switch to [branch 1.0](https://github.com/milvus-io/milvus/tree/1.0).


## Installation

### Install Milvus Standalone

Install with Docker-Compose
    

Coming soon.

Install with Helm

Coming soon.

Build from source code

```bash
# Clone github repository.
$ cd /home/$USER/
$ git clone https://github.com/milvus-io/milvus.git

# Install third-party dependencies.
$ cd /home/$USER/milvus/
$ ./scripts/install_deps.sh

# Compile Milvus standalone.
$ make milvus
```



### Install Milvus Cluster

Install with Docker-Compose

Coming soon.

Install with Helm

Coming soon.

Build from source code

```bash
# Clone github repository.
$ cd /home/$USER
$ git clone https://github.com/milvus-io/milvus.git

# Install third-party dependencies.
$ cd milvus
$ ./scripts/install_deps.sh

# Compile Milvus Cluster.
$ make milvus
```



## Milvus 2.0 is better than Milvus 1.x

<table class="comparison">
	<tr>
		<th>&nbsp;</th>
		<th><b>Milvus 1.x</b></th>
		<th><b>Milvus 2.0</b></th>
	</tr>
	<tr>
		<td><b>Architecture</b></td>
		<td>Shared storage</td>
		<td>Cloud native</td>
	</tr>
	<tr>
		<td><b>Scalability</b></td>
		<td>1 to 32 read nodes with only one write node</td>
		<td>500+ nodes</td>
	</tr>
  	<tr>
		<td><b>Durability</b></td>
		<td><li>Local disk</li><li>Network file system (NFS)</li></td>
		<td><li>Object storage service (OSS)</li><li>Distributed file system (DFS)</li></td>
	</tr>
  	<tr>
		<td><b>Availability</b></td>
		<td>99%</td>
		<td>99.9%</td>
	</tr>
	<tr>
		<td><b>Data consistency</b></td>
		<td>Eventual consistency</td>
		<td>Three levels of consistency:<li>Strong</li><li>Session</li><li>Consistent prefix</li></td>
	</tr>
	<tr>
		<td><b>Data types supported</b></td>
		<td>Vectors</td>
		<td><li>Vectors</li><li>Fixed-length scalars</li><li>String and text (in planning)</li></td>
	</tr>
	<tr>
		<td><b>Basic operations supported</b></td>
		<td><li>Data insertion</li><li>Data deletion</li><li>Approximate nearest neighbor (ANN) Search</li></td>
		<td><li>Data insertion</li><li>Data deletion (in planning)</li><li>Data query</li><li>Approximate nearest neighbor (ANN) Search</li><li>Recurrent neural network (RNN) search (in planning)</li></td>
	</tr>
	<tr>
		<td><b>Advanced features</b></td>
		<td><li>Mishards</li><li>Milvus DM</li></td>
		<td><li>Scalar filtering</li><li>Time Travel</li><li>Multi-site deployment and multi-cloud integration</li><li>Data management tools</li></td>
	</tr>
	<tr>
		<td><b>Index types and libraries</b></td>
		<td><li>Faiss</li><li>Annoy</li><li>Hnswlib</li><li>RNSG</li></td>
		<td><li>Faiss</li><li>Annoy</li><li>Hnswlib</li><li>RNSG</li><li>ScaNN (in planning)</li><li>On-disk index (in planning)</li></td>
	</tr>
	<tr>
		<td><b>SDKs</b></td>
		<td><li>Python</li><li>Java</li><li>Go</li><li>RESTful</li><li>C++</li></td>
		<td><li>Python</li><li>Go (in planning)</li><li>RESTful (in planning)</li><li>C++ (in planning)</li></td>
	</tr>
	<tr>
		<td><b>Release status</b></td>
		<td>Long-term support (LTS)</td>
		<td>Release candidate. A stable version will be released in August.</td>
	</tr>
</table>

## Getting Started

### Demos

<table>
  <tr>
    <td width="30%">
      <a href="https://zilliz.com/milvus-demos">
        <img src="https://zillizstorage.blob.core.windows.net/zilliz-assets/zilliz-assets/assets/image_search_59a64e4f22.gif" />
      </a>
    </td>
    <td width="30%">
<a href="https://zilliz.com/milvus-demos">
<img src="https://zillizstorage.blob.core.windows.net/zilliz-assets/zilliz-assets/assets/qa_df5ee7bd83.gif" />
</a>
    </td>
    <td width="30%">
<a href="https://zilliz.com/milvus-demos">
<img src="https://zillizstorage.blob.core.windows.net/zilliz-assets/zilliz-assets/assets/mole_search_76f8340572.gif" />
</a>
    </td>
  </tr>
  <tr>
    <th>
      <a href="https://zilliz.com/milvus-demos">Image search</a>
    </th>
    <th>
      <a href="https://zilliz.com/milvus-demos">Chatbots</a>
    </th>
    <th>
      <a href="https://zilliz.com/milvus-demos">Chemical structure search</a>
    </th>
  </tr>
</table>


- [Image Search](https://zilliz.com/milvus-demos)

  Images made searchable. Instantaneously return the most similar images from a massive database.

- [Chatbots](https://zilliz.com/milvus-demos)

  Interactive digital customer service that saves users time and businesses money.

- [Chemical Structure Search](https://zilliz.com/milvus-demos)

  Blazing fast similarity search, substructure search, or superstructure search for a specified molecule.


## Bootcamps

Milvus [bootcamp](https://github.com/milvus-io/bootcamp/tree/new-bootcamp) are designed to expose users to both the simplicity and depth of the vector database. Discover how to run benchmark tests as well as build similarity search applications spanning chatbots, recommendation systems, reverse image search, molecular search, and much more.


## Contributing

Contributions to Milvus are welcome from everyone. See [Guidelines for Contributing](https://github.com/milvus-io/milvus/blob/master/CONTRIBUTING.md) for details on submitting patches and the contribution workflow. See our [community repository](https://github.com/milvus-io/community) to learn about our governance and access more community resources.

## Documentation



### SDK

The implemented SDK and its API documentation are listed below:

- [PyMilvus-ORM](https://github.com/milvus-io/pymilvus-orm)



## Community

Join the Milvus community on [Slack](https://join.slack.com/t/milvusio/shared_invite/zt-e0u4qu3k-bI2GDNys3ZqX1YCJ9OM~GQ) to share your suggestions, advice, and questions with our engineering team. 

<a href="https://join.slack.com/t/milvusio/shared_invite/zt-e0u4qu3k-bI2GDNys3ZqX1YCJ9OM~GQ">
    <img src="https://zillizstorage.blob.core.windows.net/zilliz-assets/zilliz-assets/assets/readme_slack_4a07c4c92f.png" alt="Miluvs Slack Channel"  height="150" width="500">
</a>

You can also check out our [FAQ page](https://milvus.io/docs/v1.0.0/performance_faq.md) to discover solutions or answers to your issues or questions.

Subscribe to our mailing lists:

- [Milvus Technical Steering Committee](https://lists.lfai.foundation/g/milvus-tsc)
- [Milvus Technical Discussions](https://lists.lfai.foundation/g/milvus-technical-discuss)
- [Milvus Announcement](https://lists.lfai.foundation/g/milvus-announce)

Follow us on social media:

- [Milvus Medium](https://medium.com/@milvusio)
- [Milvus Twitter](https://twitter.com/milvusio)

## Join Us

Zilliz, the company behind Milvus, is [actively hiring](https://app.mokahr.com/apply/zilliz/37974#/) full-stack developers and solution engineers to build the next-generation open-source data fabric.

## Acknowledgments

Milvus adopts dependencies from the following:

- Thank [FAISS](https://github.com/facebookresearch/faiss) for the excellent search library.
- Thank [etcd](https://github.com/coreos/etcd) for providing some great open-source tools.
- Thank [Pulsar](https://github.com/apache/pulsar) for its great distributed information pub/sub platform.
- Thank [RocksDB](https://github.com/facebook/rocksdb) for the powerful storage engines.

<br><!-- Do not remove start of hero-bot --><br><img src="https://img.shields.io/badge/Total-121-orange"><br><a href="https://github.com/ABNER-1"><img src="https://avatars.githubusercontent.com/u/24547351?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/Accagain2014"><img src="https://avatars.githubusercontent.com/u/9635216?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/AllenYu1987"><img src="https://avatars.githubusercontent.com/u/12489985?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/Aredcap"><img src="https://avatars.githubusercontent.com/u/40494761?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/Ben-Aaron-Bio-Rad"><img src="https://avatars.githubusercontent.com/u/54123439?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/Bennu-Li"><img src="https://avatars.githubusercontent.com/u/53458891?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/BossZou"><img src="https://avatars.githubusercontent.com/u/40255591?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/CrossRaynor"><img src="https://avatars.githubusercontent.com/u/3909908?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/Cupchen"><img src="https://avatars.githubusercontent.com/u/34762375?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/DanielHuang1983"><img src="https://avatars.githubusercontent.com/u/4417873?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/DragonDriver"><img src="https://avatars.githubusercontent.com/u/31589260?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/Fierralin"><img src="https://avatars.githubusercontent.com/u/8857059?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/FluorineDog"><img src="https://avatars.githubusercontent.com/u/15663612?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/Gracieeea"><img src="https://avatars.githubusercontent.com/u/50101579?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/GuanyunFeng"><img src="https://avatars.githubusercontent.com/u/40229765?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/GuoRentong"><img src="https://avatars.githubusercontent.com/u/57477222?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/Heisenberg-Y"><img src="https://avatars.githubusercontent.com/u/35055583?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/HesterG"><img src="https://avatars.githubusercontent.com/u/17645053?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/HuangHua"><img src="https://avatars.githubusercontent.com/u/2274405?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/JackLCL"><img src="https://avatars.githubusercontent.com/u/53512883?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/JinHai-CN"><img src="https://avatars.githubusercontent.com/u/33142505?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/Lin-gh-Saint"><img src="https://avatars.githubusercontent.com/u/64019322?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/LocoRichard"><img src="https://avatars.githubusercontent.com/u/81553353?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/MXDA"><img src="https://avatars.githubusercontent.com/u/47274057?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/PahudPlus"><img src="https://avatars.githubusercontent.com/u/64403786?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/ReigenAraka"><img src="https://avatars.githubusercontent.com/u/57280231?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/RyanWei"><img src="https://avatars.githubusercontent.com/u/9876551?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/SCKCZJ2018"><img src="https://avatars.githubusercontent.com/u/29282370?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/SkyYang"><img src="https://avatars.githubusercontent.com/u/4702509?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/SnowyOwl-KHY"><img src="https://avatars.githubusercontent.com/u/10348819?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/SwaggySong"><img src="https://avatars.githubusercontent.com/u/36157116?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/ThreadDao"><img src="https://avatars.githubusercontent.com/u/27288593?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/Tlincy"><img src="https://avatars.githubusercontent.com/u/11934432?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/XuPeng-SH"><img src="https://avatars.githubusercontent.com/u/39627130?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/XuanYang-cn"><img src="https://avatars.githubusercontent.com/u/51370125?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/Yukikaze-CZR"><img src="https://avatars.githubusercontent.com/u/48198922?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/aaronjin2010"><img src="https://avatars.githubusercontent.com/u/48044391?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/akihoni"><img src="https://avatars.githubusercontent.com/u/36330442?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/anchun"><img src="https://avatars.githubusercontent.com/u/2356895?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/ashyshyshyman"><img src="https://avatars.githubusercontent.com/u/50362613?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/become-nice"><img src="https://avatars.githubusercontent.com/u/56624819?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/bigsheeper"><img src="https://avatars.githubusercontent.com/u/42060877?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/binbin12580"><img src="https://avatars.githubusercontent.com/u/30914966?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/binbinlv"><img src="https://avatars.githubusercontent.com/u/83755740?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/bo-huang"><img src="https://avatars.githubusercontent.com/u/24309515?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/break2017"><img src="https://avatars.githubusercontent.com/u/2993941?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/caosiyang"><img src="https://avatars.githubusercontent.com/u/2155120?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/chengpu"><img src="https://avatars.githubusercontent.com/u/2233492?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/codacy-badger"><img src="https://avatars.githubusercontent.com/u/23704769?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/congqixia"><img src="https://avatars.githubusercontent.com/u/84113973?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/cqy123456"><img src="https://avatars.githubusercontent.com/u/39671710?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/cxie"><img src="https://avatars.githubusercontent.com/u/653101?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/cydrain"><img src="https://avatars.githubusercontent.com/u/3992404?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/czhen-zilliz"><img src="https://avatars.githubusercontent.com/u/83751452?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/czpmango"><img src="https://avatars.githubusercontent.com/u/26356194?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/czs007"><img src="https://avatars.githubusercontent.com/u/59249785?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/dd-He"><img src="https://avatars.githubusercontent.com/u/24242249?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/del-zhenwu"><img src="https://avatars.githubusercontent.com/u/56623710?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/dvzubarev"><img src="https://avatars.githubusercontent.com/u/14878830?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/dyhyfu"><img src="https://avatars.githubusercontent.com/u/64584368?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/erdustiggen"><img src="https://avatars.githubusercontent.com/u/25433850?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/feisiyicl"><img src="https://avatars.githubusercontent.com/u/64510805?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/fishpenguin"><img src="https://avatars.githubusercontent.com/u/49153041?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/ggaaooppeenngg"><img src="https://avatars.githubusercontent.com/u/4769989?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/godchen0212"><img src="https://avatars.githubusercontent.com/u/67679556?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/gracezzzzz"><img src="https://avatars.githubusercontent.com/u/56617657?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/grtoverflow"><img src="https://avatars.githubusercontent.com/u/8500564?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/gujun720"><img src="https://avatars.githubusercontent.com/u/53246671?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/guoxiangzhou"><img src="https://avatars.githubusercontent.com/u/52496626?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/hadim"><img src="https://avatars.githubusercontent.com/u/528003?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/jackyu2020"><img src="https://avatars.githubusercontent.com/u/64533877?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/jeffoverflow"><img src="https://avatars.githubusercontent.com/u/24581746?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/jielinxu"><img src="https://avatars.githubusercontent.com/u/52057195?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/jkx8fc"><img src="https://avatars.githubusercontent.com/u/31717785?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/lee-eve"><img src="https://avatars.githubusercontent.com/u/9720105?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/loguo"><img src="https://avatars.githubusercontent.com/u/15364733?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/lwglgy"><img src="https://avatars.githubusercontent.com/u/26682620?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/mausch"><img src="https://avatars.githubusercontent.com/u/95194?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/mileyzjq"><img src="https://avatars.githubusercontent.com/u/37039827?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/moe-of-faith"><img src="https://avatars.githubusercontent.com/u/5696721?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/nameczz"><img src="https://avatars.githubusercontent.com/u/20559208?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/natoka"><img src="https://avatars.githubusercontent.com/u/1751024?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/neza2017"><img src="https://avatars.githubusercontent.com/u/34152706?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/op-hunter"><img src="https://avatars.githubusercontent.com/u/5617677?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/pengjeck"><img src="https://avatars.githubusercontent.com/u/14035577?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/phantom8548"><img src="https://avatars.githubusercontent.com/u/11576622?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/qbzenker"><img src="https://avatars.githubusercontent.com/u/51972064?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/sahuang"><img src="https://avatars.githubusercontent.com/u/26035292?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/scsven"><img src="https://avatars.githubusercontent.com/u/12595343?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/shana0325"><img src="https://avatars.githubusercontent.com/u/33335490?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/shanghaikid"><img src="https://avatars.githubusercontent.com/u/185051?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/shengjh"><img src="https://avatars.githubusercontent.com/u/46514371?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/shengjun1985"><img src="https://avatars.githubusercontent.com/u/49774184?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/shiyu09"><img src="https://avatars.githubusercontent.com/u/39143280?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/shiyu22"><img src="https://avatars.githubusercontent.com/u/53459423?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/siriusctrl"><img src="https://avatars.githubusercontent.com/u/26541600?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/snyk-bot"><img src="https://avatars.githubusercontent.com/u/19733683?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/sre-ci-robot"><img src="https://avatars.githubusercontent.com/u/56469371?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/sunby"><img src="https://avatars.githubusercontent.com/u/9817127?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/talentAN"><img src="https://avatars.githubusercontent.com/u/17634030?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/taydy"><img src="https://avatars.githubusercontent.com/u/24822588?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/thywdy"><img src="https://avatars.githubusercontent.com/u/56624359?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/tinkerlin"><img src="https://avatars.githubusercontent.com/u/13817362?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/wangting0128"><img src="https://avatars.githubusercontent.com/u/26307815?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/water32"><img src="https://avatars.githubusercontent.com/u/13234561?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/weishuo2"><img src="https://avatars.githubusercontent.com/u/27938020?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/wscxyey"><img src="https://avatars.githubusercontent.com/u/48882296?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/xiaocai2333"><img src="https://avatars.githubusercontent.com/u/46207236?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/xige-16"><img src="https://avatars.githubusercontent.com/u/20124155?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/xudalin0609"><img src="https://avatars.githubusercontent.com/u/35444753?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/yamasite"><img src="https://avatars.githubusercontent.com/u/10089260?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/yanliang567"><img src="https://avatars.githubusercontent.com/u/82361606?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/yhmo"><img src="https://avatars.githubusercontent.com/u/2282099?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/yiuluchen"><img src="https://avatars.githubusercontent.com/u/23047684?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/youny626"><img src="https://avatars.githubusercontent.com/u/9016120?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/yxm1536"><img src="https://avatars.githubusercontent.com/u/62009483?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/zengxy"><img src="https://avatars.githubusercontent.com/u/11961641?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/zerowe-seven"><img src="https://avatars.githubusercontent.com/u/57790060?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/zhoubo0317"><img src="https://avatars.githubusercontent.com/u/51948620?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/zwd1208"><img src="https://avatars.githubusercontent.com/u/15153901?v=4" class="avatar-user" width="40px" /></a><a href="https://github.com/zxf2017"><img src="https://avatars.githubusercontent.com/u/29620478?v=4" class="avatar-user" width="40px" /></a><br><!-- Do not remove end of hero-bot --><br>