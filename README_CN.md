<img src="https://zillizstorage.blob.core.windows.net/zilliz-assets/zilliz-assets/assets/readme_ch_962480ccfb.png" alt="Milvus banner">




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

# 欢迎来到 Milvus

## Milvus 是什么

Milvus 是一款全球领先的开源向量数据库，赋能 AI 应用和向量相似度搜索，加速非结构化数据检索。用户在任何部署环境中均可获得始终如一的用户体验。

Milvus 提供单机版与分布式版：


Milvus 基于 [Apache 2.0 License](https://github.com/milvus-io/milvus/blob/master/LICENSE) 协议发布，于 2019 年 10 月正式开源，是 [LF AI & Data 基金会](https://lfaidata.foundation/) 的毕业项目。

## 产品亮点

<details>
  <summary><b>针对万亿级向量的毫秒级搜索</b></summary>
  完成万亿条向量数据搜索的平均延迟以毫秒计。
  </details>

<details>
  <summary><b>简化的非结构化数据管理</b></summary>
  <li>一整套专为数据科学工作流设计的 API。</li><li>消除笔记本、本地集群、云服务器之间的使用差异，提供始终如一的跨平台用户体验。</li><li>可以在任何场景下实现实时搜索与分析。</li>
  </details>

<details>
  <summary><b>稳定可靠的用户体验</b></summary>
  Milvus 具有故障转移和故障恢复的机制，即使服务中断，也能确保数据和应用的业务连续性。
  </details>

<details>
  <summary><b>高度可扩展，弹性伸缩</b></summary>
  组件级别的高扩展性，支持精准扩展。
  </details>

<details>
  <summary><b>混合查询</b></summary>
  除了向量以外，Milvus还支持布尔值、整型、浮点型等数据类型。在 Milvus 中，一个 collection 可以包含多个字段来代表数据特征或属性。Milvus 还支持在向量相似度检索过程中进行标量字段过滤。
  </details>

<details>
  <summary><b>基于 Lambda 架构的流批一体式数据存储</b></summary>
  Milvus 在存储数据时支持流处理和批处理两种方式，兼顾了流处理的时效性和批处理的效率。统一的对外接口使得向量相似度查询更为便捷。
  </details>

<details>
  <summary><b>广受社区支持和业界认可</b></summary>
  Milvus 项目在 GitHub 上获星超 6000，拥有逾 1000 家企业用户，还有活跃的开源社区。Milvus 由 <a href="https://lfaidata.foundation/">LF AI & Data 基金会</a> 背书，是该基金会的毕业项目。
  </details>

>  **注意** 主分支用于 Milvus v2.0 代码开发。Milvus v1.0 于 2021 年 3 月 9 日发布，是 Milvus 的首个长期支持（LTS）版本。如需使用 Milvus 1.0，请切换至 [1.0 分支](https://github.com/milvus-io/milvus/tree/1.0)。

## 安装

### 安装 Milvus 单机版

使用 Docker-Compose 安装

敬请期待。

使用 Helm Chart 安装

敬请期待。

从源码编译 Milvus

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



### 安装 Milvus 分布式版

使用 Docker-Compose 安装

敬请期待。

使用 Helm Chart 安装

敬请期待。

从源码编译 Milvus

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



## Milvus 2.0：功能增加、性能升级

<table class="demo">
	<tr>
		<th>&nbsp;</th>
		<th><b>Milvus 1.x</b></th>
		<th><b>Milvus 2.0</b></th>
	</tr>
	<tr>
		<td><b>架构</b></td>
		<td>共享存储</td>
		<td>云原生</td>
	</tr>
	<tr>
		<td><b>可扩展性</b></td>
		<td>1 - 32 个读节点，1 个写节点</td>
		<td>500+ 个节点</td>
	</tr>
  	<tr>
		<td><b>持久性</b></td>
		<td><li>本地磁盘</li><li>网络文件系统 (NFS)</li></td>
		<td><li>对象存储 (OSS)</li><li>分布式文件系统 (DFS)</li></td>
	</tr>
  	<tr>
		<td><b>可用性</b></td>
		<td>99%</td>
		<td>99.9%</td>
	</tr>
	<tr>
		<td><b>数据一致性</b></td>
		<td>最终一致</td>
		<td>多种一致性<li>Strong</li><li>Session</li><li>Consistent prefix</li></td>
	</tr>
	<tr>
		<td><b>数据类型支持</b></td>
		<td>向量数据</td>
		<td><li>向量数据</li><li>标量数据</li><li>字符串与文本 (开发中)</li></td>
	</tr>
	<tr>
		<td><b>基本操作</b></td>
		<td><li>插入数据</li><li>删除数据</li><li>相似最邻近（ANN）搜索</li></td>
		<td><li>插入数据</li><li>删除数据 (开发中)</li><li>数据查询</li><li>相似最邻近（ANN）搜索</li><li>基于半径的最近邻算法（RNN） (开发中)</li></td>
	</tr>
	<tr>
		<td><b>高级功能</b></td>
		<td><li>Mishards</li><li>Milvus DM 数据迁移工具</li></td>
		<td><li>标量字段过滤</li><li>Time Travel</li><li>多云/地域部署</li><li>数据管理工具</li></td>
	</tr>
	<tr>
		<td><b>索引类型</b></td>
		<td><li>Faiss</li><li>Annoy</li><li>Hnswlib</li><li>RNSG</li></td>
		<td><li>Faiss</li><li>Annoy</li><li>Hnswlib</li><li>RNSG</li><li>ScaNN (开发中)</li><li>On-disk index (开发中)</li></td>
	</tr>
	<tr>
		<td><b>SDK</b></td>
		<td><li>Python</li><li>Java</li><li>Go</li><li>RESTful</li><li>C++</li></td>
		<td><li>Python</li><li>Go (开发中)</li><li>RESTful (开发中)</li><li>C++ (开发中)</li></td>
	</tr>
	<tr>
		<td><b>当前状态</b></td>
		<td>长期支持（LTS）版本</td>
		<td>预览版本。预计 2021 年 8 月发布稳定版本。</td>
	</tr>
</table>

## 入门指南

### 应用场景

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
      <a href="https://zilliz.com/milvus-demos">以图搜图系统</a>
    </th>
    <th>
      <a href="https://zilliz.com/milvus-demos">智能问答机器人</a>
    </th>
    <th>
      <a href="https://zilliz.com/milvus-demos">分子式检索系统</a>
    </th>
  </tr>
</table>


- [以图搜图系统](https://zilliz.com/milvus-demos)：从海量图片中快速检索最相似图片。

- [智能问答机器人](https://zilliz.com/milvus-demos)：交互式智能问答机器人帮助用户节省时间和用人成本。

- [分子式检索系统](https://zilliz.com/milvus-demos)：迅速检索相似化学分子式。



## 训练营

Milvus 训练营能够帮助你了解向量数据库的操作及各种应用场景。通过 Milvus 训练营探索如何进行 Milvus 性能测评，搭建智能问答机器人、推荐系统、以图搜图系统、分子式检索系统等。

## 贡献代码

欢迎向 Milvus 社区贡献你的代码。代码贡献流程或提交补丁等相关信息详见 [代码贡献准则](https://github.com/milvus-io/milvus/blob/master/CONTRIBUTING.md)。参考 [社区仓库](https://github.com/milvus-io/community) 了解社区管理准则并获取更多社区资源。

<br><!-- Do not remove start of hero-bot --><br><br><!-- Do not remove end of hero-bot --><br>

## Milvus 文档

### SDK

- [PyMilvus-ORM](https://github.com/milvus-io/pymilvus-orm)



## 社区

欢迎加入 [Slack](https://join.slack.com/t/milvusio/shared_invite/zt-e0u4qu3k-bI2GDNys3ZqX1YCJ9OM~GQ) 频道分享你的建议与问题。你也可以通过 [FAQ](https://milvus.io/cn/docs/v1.0.0/performance_faq.md) 页面，查看常见问题及解答。 

订阅 Milvus 邮件：

- [Milvus Technical Steering Committee](https://lists.lfai.foundation/g/milvus-tsc)
- [Milvus Technical Discussions](https://lists.lfai.foundation/g/milvus-technical-discuss)
- [Milvus Announcement](https://lists.lfai.foundation/g/milvus-announce)

关注我们的社交媒体：

- [知乎](https://zilliz.atlassian.net/wiki/spaces/TC/pages/251658753/CN%2BTranslation%2BWhat%2Bis%2BMilvus#)
- [CSDN](http://zilliz.blog.csdn.net/)
- [Bilibili](http://space.bilibili.com/478166626)
- Zilliz 技术交流微信群

<img src="https://zillizstorage.blob.core.windows.net/zilliz-assets/zilliz-assets/assets/wechat_2abac21f5a.png" alt="Wechat QR Code">

## 加入我们

Zilliz 是 Milvus 项目的幕后公司。我们正在 [招聘](https://app.mokahr.com/apply/zilliz/37974#/) 算法、开发和全栈工程师。欢迎加入我们，让我们携手构建下一代的开源数据基础软件。

## 特别感谢

Milvus 采用了以下依赖库:

- 感谢 [FAISS](https://github.com/facebookresearch/faiss) 相似性检索库。
- 感谢开源键值存储 [etcd](https://github.com/coreos/etcd)。
- 感谢分布式信息发布/订阅平台 [Pulsar](https://github.com/apache/pulsar)。
- 感谢存储引擎 [RocksDB](https://github.com/facebook/rocksdb)。

