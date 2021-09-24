<img src="https://zilliz-cms.s3.us-west-2.amazonaws.com/readme_ch_69fbf0fc45.png" alt="Milvus banner">

<div class="column" align="middle">
  <a href="https://join.slack.com/t/milvusio/shared_invite/zt-e0u4qu3k-bI2GDNys3ZqX1YCJ9OM~GQ">
    <img src="https://img.shields.io/badge/Join-Slack-orange" alt="join-slack"/>
  </a>
  <img src="https://img.shields.io/github/license/milvus-io/milvus" alt="license"/>
  <img src="https://img.shields.io/docker/pulls/milvusdb/milvus" alt="docker-pull-count" />
</div>

<div class="column" align="middle">
  <a href="https://bestpractices.coreinfrastructure.org/projects/3563">
    <img src="https://bestpractices.coreinfrastructure.org/projects/3563/badge" />
  </a>
  <a href="https://app.codacy.com/gh/milvus-io/milvus?utm_source=github.com&utm_medium=referral&utm_content=milvus-io/milvus&utm_campaign=Badge_Grade_Dashboard">
    <img src="https://api.codacy.com/project/badge/Grade/c4bb2ccfb51b47f99e43bfd1705edd95" />
  </a>
</div>

# 欢迎来到 Milvus

## Milvus 是什么

Milvus 是一款全球领先的开源向量数据库，赋能 AI 应用和向量相似度搜索，加速非结构化数据检索。用户在任何部署环境中均可获得始终如一的用户体验。

Milvus 2.0 是一款云原生向量数据库，采用存储与计算分离的架构设计。该重构版本的所有组件均为无状态组件，极大地增强了系统弹性和灵活性。更多系统架构细节，参考 [Milvus 系统架构](https://milvus.io/cn/docs/v2.0.0/architecture_overview.md)。

Milvus 基于 [Apache 2.0 License](https://github.com/milvus-io/milvus/blob/master/LICENSE) 协议发布，于 2019 年 10 月正式开源，是 [LF AI & Data 基金会](https://lfaidata.foundation/) 的毕业项目。

## 产品亮点

<details>
  <summary><b>针对万亿级向量的毫秒级搜索</b></summary>
  完成万亿条向量数据搜索的平均延迟以毫秒计。
  </details>

<details>
  <summary><b>简化的非结构化数据管理</b></summary>
  <li>一整套专为数据科学工作流设计的 API。</li><li>无论是笔记本、本地集群还是云服务器，始终如一的跨平台用户体验。</li><li>可以在任何场景下实现实时搜索与分析。</li>
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
  除了向量以外，Milvus还支持布尔值、整型、浮点等数据类型。在 Milvus 中，一个 collection 可以包含多个字段来代表数据特征或属性。Milvus 还支持在向量相似度检索过程中进行标量字段过滤。
  </details>

<details>
  <summary><b>基于 Lambda 架构的流批一体式数据存储</b></summary>
  Milvus 在存储数据时支持流处理和批处理两种方式，兼顾了流处理的时效性和批处理的效率。统一的对外接口使得向量相似度查询更为便捷。
  </details>

<details>
  <summary><b>广受社区支持和业界认可</b></summary>
  Milvus 项目在 GitHub 上获星超 7900，拥有逾 1000 家企业用户，还有活跃的开源社区。Milvus 由 <a href="https://lfaidata.foundation/">LF AI & Data 基金会</a> 背书，是该基金会的毕业项目。
  </details>

## 快速开始

### 开始使用 Milvus

- [Standalone Quick Start Guide](https://milvus.io/cn/docs/v2.0.0/install_standalone-docker.md)

- [Cluster Quick Start Guide](https://milvus.io/cn/docs/v2.0.0/install_cluster-docker.md)

### 开发 Milvus

```bash
# Clone github repository.
$ git clone https://github.com/milvus-io/milvus.git

# Install third-party dependencies.
$ cd milvus/
$ ./scripts/install_deps.sh

# Compile Milvus standalone.
$ make milvus
```

获取更多内容，请阅读 [开发者文档](https://github.com/milvus-io/milvus/blob/master/DEVELOPMENT.md)

> **注意** 主分支用于 Milvus v2.0 代码开发。Milvus v1.0 于 2021 年 3 月 9 日发布，是 Milvus 的首个长期支持（LTS）版本。如需使用 Milvus 1.0，请切换至 [1.0 分支](https://github.com/milvus-io/milvus/tree/1.0)。

## Milvus 2.0：功能增加、性能升级

详见 [Milvus 2.0 vs. 1.x](https://github.com/milvus-io/milvus/blob/master/milvus20vs1x.md) 。

## 入门指南

### 应用场景

<table>
  <tr>
    <td width="30%">
      <a href="https://zilliz.com/milvus-demos">
        <img src="https://zilliz-cms.s3.us-west-2.amazonaws.com/image_search_59a64e4f22.gif" />
      </a>
    </td>
    <td width="30%">
<a href="https://zilliz.com/milvus-demos">
<img src="https://zilliz-cms.s3.us-west-2.amazonaws.com/qa_df5ee7bd83.gif" />
</a>
    </td>
    <td width="30%">
<a href="https://zilliz.com/milvus-demos">
<img src="https://zilliz-cms.s3.us-west-2.amazonaws.com/mole_search_76f8340572.gif" />
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

#### 以图搜图系统

从海量图片中快速检索最相似图片。

#### 智能问答机器人

交互式智能问答机器人帮助用户节省时间和用人成本。

#### 分子式检索系统

迅速检索相似化学分子式。

## 训练营

Milvus [训练营](https://github.com/milvus-io/bootcamp)能够帮助你了解向量数据库的操作及各种应用场景。通过 Milvus 训练营探索如何进行 Milvus 性能测评，搭建智能问答机器人、推荐系统、以图搜图系统、分子式检索系统等。

## 贡献代码

欢迎向 Milvus 社区贡献你的代码。代码贡献流程或提交补丁等相关信息详见 [代码贡献准则](https://github.com/milvus-io/milvus/blob/master/CONTRIBUTING.md)。参考 [社区仓库](https://github.com/milvus-io/community) 了解社区管理准则并获取更多社区资源。

### All contributors

<br><!-- Do not remove start of hero-bot -->
<img src="https://img.shields.io/badge/all--contributors-170-orange"><br>
<a href="https://github.com/ABNER-1"><img src="https://avatars.githubusercontent.com/u/24547351?v=4" width="30px" /></a>
<a href="https://github.com/Accagain2014"><img src="https://avatars.githubusercontent.com/u/9635216?v=4" width="30px" /></a>
<a href="https://github.com/AllenYu1987"><img src="https://avatars.githubusercontent.com/u/12489985?v=4" width="30px" /></a>
<a href="https://github.com/Aredcap"><img src="https://avatars.githubusercontent.com/u/40494761?v=4" width="30px" /></a>
<a href="https://github.com/Ben-Aaron-Bio-Rad"><img src="https://avatars.githubusercontent.com/u/54123439?v=4" width="30px" /></a>
<a href="https://github.com/Bennu-Li"><img src="https://avatars.githubusercontent.com/u/53458891?v=4" width="30px" /></a>
<a href="https://github.com/BossZou"><img src="https://avatars.githubusercontent.com/u/40255591?v=4" width="30px" /></a>
<a href="https://github.com/Chisdo"><img src="https://avatars.githubusercontent.com/u/36720318?v=4" width="30px" /></a>
<a href="https://github.com/CrossRaynor"><img src="https://avatars.githubusercontent.com/u/3909908?v=4" width="30px" /></a>
<a href="https://github.com/Cupchen"><img src="https://avatars.githubusercontent.com/u/34762375?v=4" width="30px" /></a>
<a href="https://github.com/DanielHuang1983"><img src="https://avatars.githubusercontent.com/u/4417873?v=4" width="30px" /></a>
<a href="https://github.com/DragonDriver"><img src="https://avatars.githubusercontent.com/u/31589260?v=4" width="30px" /></a>
<a href="https://github.com/Emma-Song"><img src="https://avatars.githubusercontent.com/u/64460989?v=4" width="30px" /></a>
<a href="https://github.com/Fierralin"><img src="https://avatars.githubusercontent.com/u/8857059?v=4" width="30px" /></a>
<a href="https://github.com/FluorineDog"><img src="https://avatars.githubusercontent.com/u/15663612?v=4" width="30px" /></a>
<a href="https://github.com/Giriraj-Roy"><img src="https://avatars.githubusercontent.com/u/88903134?v=4" width="30px" /></a>
<a href="https://github.com/Googleaa"><img src="https://avatars.githubusercontent.com/u/55842817?v=4" width="30px" /></a>
<a href="https://github.com/Gracieeea"><img src="https://avatars.githubusercontent.com/u/50101579?v=4" width="30px" /></a>
<a href="https://github.com/GuanyunFeng"><img src="https://avatars.githubusercontent.com/u/40229765?v=4" width="30px" /></a>
<a href="https://github.com/GuoRentong"><img src="https://avatars.githubusercontent.com/u/57477222?v=4" width="30px" /></a>
<a href="https://github.com/Heisenberg-Y"><img src="https://avatars.githubusercontent.com/u/35055583?v=4" width="30px" /></a>
<a href="https://github.com/HesterG"><img src="https://avatars.githubusercontent.com/u/17645053?v=4" width="30px" /></a>
<a href="https://github.com/HuangHua"><img src="https://avatars.githubusercontent.com/u/2274405?v=4" width="30px" /></a>
<a href="https://github.com/JackLCL"><img src="https://avatars.githubusercontent.com/u/53512883?v=4" width="30px" /></a>
<a href="https://github.com/JinHai-CN"><img src="https://avatars.githubusercontent.com/u/33142505?v=4" width="30px" /></a>
<a href="https://github.com/Lin-gh-Saint"><img src="https://avatars.githubusercontent.com/u/64019322?v=4" width="30px" /></a>
<a href="https://github.com/LionelDong"><img src="https://avatars.githubusercontent.com/u/7533395?v=4" width="30px" /></a>
<a href="https://github.com/LocoRichard"><img src="https://avatars.githubusercontent.com/u/81553353?v=4" width="30px" /></a>
<a href="https://github.com/LoveEachDay"><img src="https://avatars.githubusercontent.com/u/1573213?v=4" width="30px" /></a>
<a href="https://github.com/MXDA"><img src="https://avatars.githubusercontent.com/u/47274057?v=4" width="30px" /></a>
<a href="https://github.com/NotRyan"><img src="https://avatars.githubusercontent.com/u/5742796?v=4" width="30px" /></a>
<a href="https://github.com/PahudPlus"><img src="https://avatars.githubusercontent.com/u/64403786?v=4" width="30px" /></a>
<a href="https://github.com/QipengZhou"><img src="https://avatars.githubusercontent.com/u/5410298?v=4" width="30px" /></a>
<a href="https://github.com/ReigenAraka"><img src="https://avatars.githubusercontent.com/u/57280231?v=4" width="30px" /></a>
<a href="https://github.com/RyanWei"><img src="https://avatars.githubusercontent.com/u/9876551?v=4" width="30px" /></a>
<a href="https://github.com/SCKCZJ2018"><img src="https://avatars.githubusercontent.com/u/29282370?v=4" width="30px" /></a>
<a href="https://github.com/SkyYang"><img src="https://avatars.githubusercontent.com/u/4702509?v=4" width="30px" /></a>
<a href="https://github.com/SnowyOwl-KHY"><img src="https://avatars.githubusercontent.com/u/10348819?v=4" width="30px" /></a>
<a href="https://github.com/Sunt-ing"><img src="https://avatars.githubusercontent.com/u/43040147?v=4" width="30px" /></a>
<a href="https://github.com/SwaggySong"><img src="https://avatars.githubusercontent.com/u/36157116?v=4" width="30px" /></a>
<a href="https://github.com/ThreadDao"><img src="https://avatars.githubusercontent.com/u/27288593?v=4" width="30px" /></a>
<a href="https://github.com/ThyeeZz"><img src="https://avatars.githubusercontent.com/u/41352919?v=4" width="30px" /></a>
<a href="https://github.com/Tlincy"><img src="https://avatars.githubusercontent.com/u/11934432?v=4" width="30px" /></a>
<a href="https://github.com/Tumao727"><img src="https://avatars.githubusercontent.com/u/20420181?v=4" width="30px" /></a>
<a href="https://github.com/XuPeng-SH"><img src="https://avatars.githubusercontent.com/u/39627130?v=4" width="30px" /></a>
<a href="https://github.com/XuanYang-cn"><img src="https://avatars.githubusercontent.com/u/51370125?v=4" width="30px" /></a>
<a href="https://github.com/YiyunNi"><img src="https://avatars.githubusercontent.com/u/74396087?v=4" width="30px" /></a>
<a href="https://github.com/Yukikaze-CZR"><img src="https://avatars.githubusercontent.com/u/48198922?v=4" width="30px" /></a>
<a href="https://github.com/aaronjin2010"><img src="https://avatars.githubusercontent.com/u/48044391?v=4" width="30px" /></a>
<a href="https://github.com/akihoni"><img src="https://avatars.githubusercontent.com/u/36330442?v=4" width="30px" /></a>
<a href="https://github.com/alwayslove2013"><img src="https://avatars.githubusercontent.com/u/22510720?v=4" width="30px" /></a>
<a href="https://github.com/anchun"><img src="https://avatars.githubusercontent.com/u/2356895?v=4" width="30px" /></a>
<a href="https://github.com/ashyshyshyman"><img src="https://avatars.githubusercontent.com/u/50362613?v=4" width="30px" /></a>
<a href="https://github.com/become-nice"><img src="https://avatars.githubusercontent.com/u/56624819?v=4" width="30px" /></a>
<a href="https://github.com/bigsheeper"><img src="https://avatars.githubusercontent.com/u/42060877?v=4" width="30px" /></a>
<a href="https://github.com/binbin12580"><img src="https://avatars.githubusercontent.com/u/30914966?v=4" width="30px" /></a>
<a href="https://github.com/binbinlv"><img src="https://avatars.githubusercontent.com/u/83755740?v=4" width="30px" /></a>
<a href="https://github.com/bo-huang"><img src="https://avatars.githubusercontent.com/u/24309515?v=4" width="30px" /></a>
<a href="https://github.com/break2017"><img src="https://avatars.githubusercontent.com/u/2993941?v=4" width="30px" /></a>
<a href="https://github.com/caosiyang"><img src="https://avatars.githubusercontent.com/u/2155120?v=4" width="30px" /></a>
<a href="https://github.com/chengpu"><img src="https://avatars.githubusercontent.com/u/2233492?v=4" width="30px" /></a>
<a href="https://github.com/chenxingqiang"><img src="https://avatars.githubusercontent.com/u/12387235?v=4" width="30px" /></a>
<a href="https://github.com/claireyuw"><img src="https://avatars.githubusercontent.com/u/83751381?v=4" width="30px" /></a>
<a href="https://github.com/codacy-badger"><img src="https://avatars.githubusercontent.com/u/23704769?v=4" width="30px" /></a>
<a href="https://github.com/codenoid"><img src="https://avatars.githubusercontent.com/u/14269809?v=4" width="30px" /></a>
<a href="https://github.com/congqixia"><img src="https://avatars.githubusercontent.com/u/84113973?v=4" width="30px" /></a>
<a href="https://github.com/cqy123456"><img src="https://avatars.githubusercontent.com/u/39671710?v=4" width="30px" /></a>
<a href="https://github.com/cxie"><img src="https://avatars.githubusercontent.com/u/653101?v=4" width="30px" /></a>
<a href="https://github.com/cydrain"><img src="https://avatars.githubusercontent.com/u/3992404?v=4" width="30px" /></a>
<a href="https://github.com/czhen-zilliz"><img src="https://avatars.githubusercontent.com/u/83751452?v=4" width="30px" /></a>
<a href="https://github.com/czpmango"><img src="https://avatars.githubusercontent.com/u/26356194?v=4" width="30px" /></a>
<a href="https://github.com/czs007"><img src="https://avatars.githubusercontent.com/u/59249785?v=4" width="30px" /></a>
<a href="https://github.com/dd-He"><img src="https://avatars.githubusercontent.com/u/24242249?v=4" width="30px" /></a>
<a href="https://github.com/dddddai"><img src="https://avatars.githubusercontent.com/u/41563853?v=4" width="30px" /></a>
<a href="https://github.com/del-zhenwu"><img src="https://avatars.githubusercontent.com/u/56623710?v=4" width="30px" /></a>
<a href="https://github.com/douglarek"><img src="https://avatars.githubusercontent.com/u/1488134?v=4" width="30px" /></a>
<a href="https://github.com/dvzubarev"><img src="https://avatars.githubusercontent.com/u/14878830?v=4" width="30px" /></a>
<a href="https://github.com/dyhyfu"><img src="https://avatars.githubusercontent.com/u/64584368?v=4" width="30px" /></a>
<a href="https://github.com/erdustiggen"><img src="https://avatars.githubusercontent.com/u/25433850?v=4" width="30px" /></a>
<a href="https://github.com/feisiyicl"><img src="https://avatars.githubusercontent.com/u/64510805?v=4" width="30px" /></a>
<a href="https://github.com/filip-halt"><img src="https://avatars.githubusercontent.com/u/81822489?v=4" width="30px" /></a>
<a href="https://github.com/fishpenguin"><img src="https://avatars.githubusercontent.com/u/49153041?v=4" width="30px" /></a>
<a href="https://github.com/ggaaooppeenngg"><img src="https://avatars.githubusercontent.com/u/4769989?v=4" width="30px" /></a>
<a href="https://github.com/godchen0212"><img src="https://avatars.githubusercontent.com/u/67679556?v=4" width="30px" /></a>
<a href="https://github.com/goodhamgupta"><img src="https://avatars.githubusercontent.com/u/14368181?v=4" width="30px" /></a>
<a href="https://github.com/gracezzzzz"><img src="https://avatars.githubusercontent.com/u/56617657?v=4" width="30px" /></a>
<a href="https://github.com/grtoverflow"><img src="https://avatars.githubusercontent.com/u/8500564?v=4" width="30px" /></a>
<a href="https://github.com/gujun720"><img src="https://avatars.githubusercontent.com/u/53246671?v=4" width="30px" /></a>
<a href="https://github.com/guoxiangzhou"><img src="https://avatars.githubusercontent.com/u/52496626?v=4" width="30px" /></a>
<a href="https://github.com/hadim"><img src="https://avatars.githubusercontent.com/u/528003?v=4" width="30px" /></a>
<a href="https://github.com/ireneontheway5"><img src="https://avatars.githubusercontent.com/u/75291211?v=4" width="30px" /></a>
<a href="https://github.com/jackyu2020"><img src="https://avatars.githubusercontent.com/u/64533877?v=4" width="30px" /></a>
<a href="https://github.com/jaelgu"><img src="https://avatars.githubusercontent.com/u/86251631?v=4" width="30px" /></a>
<a href="https://github.com/jeffoverflow"><img src="https://avatars.githubusercontent.com/u/24581746?v=4" width="30px" /></a>
<a href="https://github.com/jielinxu"><img src="https://avatars.githubusercontent.com/u/52057195?v=4" width="30px" /></a>
<a href="https://github.com/jingkl"><img src="https://avatars.githubusercontent.com/u/34296482?v=4" width="30px" /></a>
<a href="https://github.com/jkx8fc"><img src="https://avatars.githubusercontent.com/u/31717785?v=4" width="30px" /></a>
<a href="https://github.com/kateshaowanjou"><img src="https://avatars.githubusercontent.com/u/58837504?v=4" width="30px" /></a>
<a href="https://github.com/lee-eve"><img src="https://avatars.githubusercontent.com/u/9720105?v=4" width="30px" /></a>
<a href="https://github.com/leonardokidd"><img src="https://avatars.githubusercontent.com/u/14940941?v=4" width="30px" /></a>
<a href="https://github.com/lhotari"><img src="https://avatars.githubusercontent.com/u/66864?v=4" width="30px" /></a>
<a href="https://github.com/linchpinlin"><img src="https://avatars.githubusercontent.com/u/31131753?v=4" width="30px" /></a>
<a href="https://github.com/lixiangMindSpore"><img src="https://avatars.githubusercontent.com/u/78945582?v=4" width="30px" /></a>
<a href="https://github.com/loguo"><img src="https://avatars.githubusercontent.com/u/15364733?v=4" width="30px" /></a>
<a href="https://github.com/lsgrep"><img src="https://avatars.githubusercontent.com/u/3893940?v=4" width="30px" /></a>
<a href="https://github.com/lwglgy"><img src="https://avatars.githubusercontent.com/u/26682620?v=4" width="30px" /></a>
<a href="https://github.com/maclandrol"><img src="https://avatars.githubusercontent.com/u/5290110?v=4" width="30px" /></a>
<a href="https://github.com/matrixji"><img src="https://avatars.githubusercontent.com/u/183388?v=4" width="30px" /></a>
<a href="https://github.com/mausch"><img src="https://avatars.githubusercontent.com/u/95194?v=4" width="30px" /></a>
<a href="https://github.com/miia12"><img src="https://avatars.githubusercontent.com/u/22544815?v=4" width="30px" /></a>
<a href="https://github.com/mileyzjq"><img src="https://avatars.githubusercontent.com/u/37039827?v=4" width="30px" /></a>
<a href="https://github.com/milvus-ci-robot"><img src="https://avatars.githubusercontent.com/u/87847967?v=4" width="30px" /></a>
<a href="https://github.com/moe-of-faith"><img src="https://avatars.githubusercontent.com/u/5696721?v=4" width="30px" /></a>
<a href="https://github.com/nameczz"><img src="https://avatars.githubusercontent.com/u/20559208?v=4" width="30px" /></a>
<a href="https://github.com/natoka"><img src="https://avatars.githubusercontent.com/u/1751024?v=4" width="30px" /></a>
<a href="https://github.com/neza2017"><img src="https://avatars.githubusercontent.com/u/34152706?v=4" width="30px" /></a>
<a href="https://github.com/op-hunter"><img src="https://avatars.githubusercontent.com/u/5617677?v=4" width="30px" /></a>
<a href="https://github.com/parsa-ra"><img src="https://avatars.githubusercontent.com/u/11356471?v=4" width="30px" /></a>
<a href="https://github.com/pengjeck"><img src="https://avatars.githubusercontent.com/u/14035577?v=4" width="30px" /></a>
<a href="https://github.com/phantom8548"><img src="https://avatars.githubusercontent.com/u/11576622?v=4" width="30px" /></a>
<a href="https://github.com/qbzenker"><img src="https://avatars.githubusercontent.com/u/51972064?v=4" width="30px" /></a>
<a href="https://github.com/rahulmistri1997"><img src="https://avatars.githubusercontent.com/u/58909377?v=4" width="30px" /></a>
<a href="https://github.com/sahuang"><img src="https://avatars.githubusercontent.com/u/26035292?v=4" width="30px" /></a>
<a href="https://github.com/scipe"><img src="https://avatars.githubusercontent.com/u/3996622?v=4" width="30px" /></a>
<a href="https://github.com/scsven"><img src="https://avatars.githubusercontent.com/u/12595343?v=4" width="30px" /></a>
<a href="https://github.com/seo-jinBro"><img src="https://avatars.githubusercontent.com/u/17746814?v=4" width="30px" /></a>
<a href="https://github.com/shana0325"><img src="https://avatars.githubusercontent.com/u/33335490?v=4" width="30px" /></a>
<a href="https://github.com/shanghaikid"><img src="https://avatars.githubusercontent.com/u/185051?v=4" width="30px" /></a>
<a href="https://github.com/shengjh"><img src="https://avatars.githubusercontent.com/u/46514371?v=4" width="30px" /></a>
<a href="https://github.com/shengjun1985"><img src="https://avatars.githubusercontent.com/u/49774184?v=4" width="30px" /></a>
<a href="https://github.com/shiyu09"><img src="https://avatars.githubusercontent.com/u/39143280?v=4" width="30px" /></a>
<a href="https://github.com/shiyu22"><img src="https://avatars.githubusercontent.com/u/53459423?v=4" width="30px" /></a>
<a href="https://github.com/sileht"><img src="https://avatars.githubusercontent.com/u/200878?v=4" width="30px" /></a>
<a href="https://github.com/siriusctrl"><img src="https://avatars.githubusercontent.com/u/26541600?v=4" width="30px" /></a>
<a href="https://github.com/snyk-bot"><img src="https://avatars.githubusercontent.com/u/19733683?v=4" width="30px" /></a>
<a href="https://github.com/sre-ci-robot"><img src="https://avatars.githubusercontent.com/u/56469371?v=4" width="30px" /></a>
<a href="https://github.com/ss892714028"><img src="https://avatars.githubusercontent.com/u/34635663?v=4" width="30px" /></a>
<a href="https://github.com/stuartjing"><img src="https://avatars.githubusercontent.com/u/3454260?v=4" width="30px" /></a>
<a href="https://github.com/sty945"><img src="https://avatars.githubusercontent.com/u/10708326?v=4" width="30px" /></a>
<a href="https://github.com/sunby"><img src="https://avatars.githubusercontent.com/u/9817127?v=4" width="30px" /></a>
<a href="https://github.com/sutcalag"><img src="https://avatars.githubusercontent.com/u/83750738?v=4" width="30px" /></a>
<a href="https://github.com/sworddish"><img src="https://avatars.githubusercontent.com/u/219938?v=4" width="30px" /></a>
<a href="https://github.com/talentAN"><img src="https://avatars.githubusercontent.com/u/17634030?v=4" width="30px" /></a>
<a href="https://github.com/taydy"><img src="https://avatars.githubusercontent.com/u/24822588?v=4" width="30px" /></a>
<a href="https://github.com/thywdy"><img src="https://avatars.githubusercontent.com/u/56624359?v=4" width="30px" /></a>
<a href="https://github.com/tinkerlin"><img src="https://avatars.githubusercontent.com/u/13817362?v=4" width="30px" /></a>
<a href="https://github.com/wangting0128"><img src="https://avatars.githubusercontent.com/u/26307815?v=4" width="30px" /></a>
<a href="https://github.com/want-fly"><img src="https://avatars.githubusercontent.com/u/36727480?v=4" width="30px" /></a>
<a href="https://github.com/water32"><img src="https://avatars.githubusercontent.com/u/13234561?v=4" width="30px" /></a>
<a href="https://github.com/weishuo2"><img src="https://avatars.githubusercontent.com/u/27938020?v=4" width="30px" /></a>
<a href="https://github.com/wscxyey"><img src="https://avatars.githubusercontent.com/u/48882296?v=4" width="30px" /></a>
<a href="https://github.com/xaxys"><img src="https://avatars.githubusercontent.com/u/28949072?v=4" width="30px" /></a>
<a href="https://github.com/xiaocai2333"><img src="https://avatars.githubusercontent.com/u/46207236?v=4" width="30px" /></a>
<a href="https://github.com/xiaofan-luan"><img src="https://avatars.githubusercontent.com/u/83447078?v=4" width="30px" /></a>
<a href="https://github.com/xige-16"><img src="https://avatars.githubusercontent.com/u/20124155?v=4" width="30px" /></a>
<a href="https://github.com/xudalin0609"><img src="https://avatars.githubusercontent.com/u/35444753?v=4" width="30px" /></a>
<a href="https://github.com/xuqiwe"><img src="https://avatars.githubusercontent.com/u/57252655?v=4" width="30px" /></a>
<a href="https://github.com/yamasite"><img src="https://avatars.githubusercontent.com/u/10089260?v=4" width="30px" /></a>
<a href="https://github.com/yanliang567"><img src="https://avatars.githubusercontent.com/u/82361606?v=4" width="30px" /></a>
<a href="https://github.com/yhmo"><img src="https://avatars.githubusercontent.com/u/2282099?v=4" width="30px" /></a>
<a href="https://github.com/yiuluchen"><img src="https://avatars.githubusercontent.com/u/23047684?v=4" width="30px" /></a>
<a href="https://github.com/youny626"><img src="https://avatars.githubusercontent.com/u/9016120?v=4" width="30px" /></a>
<a href="https://github.com/yxm1536"><img src="https://avatars.githubusercontent.com/u/62009483?v=4" width="30px" /></a>
<a href="https://github.com/zengxy"><img src="https://avatars.githubusercontent.com/u/11961641?v=4" width="30px" /></a>
<a href="https://github.com/zerowe-seven"><img src="https://avatars.githubusercontent.com/u/57790060?v=4" width="30px" /></a>
<a href="https://github.com/zhang787jun"><img src="https://avatars.githubusercontent.com/u/51014996?v=4" width="30px" /></a>
<a href="https://github.com/zhoubo0317"><img src="https://avatars.githubusercontent.com/u/51948620?v=4" width="30px" /></a>
<a href="https://github.com/zhuwenxing"><img src="https://avatars.githubusercontent.com/u/12268675?v=4" width="30px" /></a>
<a href="https://github.com/zwd1208"><img src="https://avatars.githubusercontent.com/u/15153901?v=4" width="30px" /></a>
<a href="https://github.com/zxf2017"><img src="https://avatars.githubusercontent.com/u/29620478?v=4" width="30px" /></a>
<!-- Do not remove end of hero-bot --><br>

## Milvus 文档

获取更多有关安装、开发、部署和管理的指南，请查看 [Milvus 文档](https://milvus.io/cn/docs/home).

### SDK

- [PyMilvus](https://github.com/milvus-io/pymilvus)
- [Java SDK](https://github.com/milvus-io/milvus-sdk-java)(开发中)
- [Go SDK](https://github.com/milvus-io/milvus-sdk-go)(开发中)
- [Node SDK](https://github.com/milvus-io/milvus-sdk-node)

## 社区

欢迎加入 [Slack](https://join.slack.com/t/milvusio/shared_invite/zt-e0u4qu3k-bI2GDNys3ZqX1YCJ9OM~GQ) 频道分享你的建议与问题。你也可以通过 [FAQ](https://milvus.io/cn/docs/v1.0.0/performance_faq.md) 页面，查看常见问题及解答。

订阅 Milvus 邮件：

- [Technical Steering Committee](https://lists.lfai.foundation/g/milvus-tsc)
- [Technical Discussions](https://lists.lfai.foundation/g/milvus-technical-discuss)
- [Announcement](https://lists.lfai.foundation/g/milvus-announce)

关注 Milvus 社交媒体：

- [知乎](https://zilliz.atlassian.net/wiki/spaces/TC/pages/251658753/CN%2BTranslation%2BWhat%2Bis%2BMilvus#)
- [CSDN](http://zilliz.blog.csdn.net/)
- [Bilibili](http://space.bilibili.com/478166626)
- Zilliz 技术交流微信群

<img src="https://zilliz-cms.s3.us-west-2.amazonaws.com/wechat_2abac21f5a.png" alt="Wechat QR Code">

## 加入我们

Zilliz 是 Milvus 项目的幕后公司。我们正在 [招聘](https://app.mokahr.com/apply/zilliz/37974#/) 算法、开发和全栈工程师。欢迎加入我们，让我们携手构建下一代的开源数据基础软件。

## 引用

如需在研究论文中使用 Milvus，欢迎添加以下引用：

```
@inproceedings{2021milvus,
  title={Milvus: A Purpose-Built Vector Data Management System},
  author={Wang, Jianguo and Yi, Xiaomeng and Guo, Rentong and Jin, Hai and Xu, Peng and Li, Shengjun and Wang, Xiangyu and Guo, Xiangzhou and Li, Chengming and Xu, Xiaohai and others},
  booktitle={Proceedings of the 2021 International Conference on Management of Data},
  pages={2614--2627},
  year={2021}
}
```

## 特别感谢

Milvus 采用了以下依赖库:

- 感谢 [FAISS](https://github.com/facebookresearch/faiss) 相似性检索库。
- 感谢开源键值存储 [etcd](https://github.com/coreos/etcd)。
- 感谢分布式信息发布/订阅平台 [Pulsar](https://github.com/apache/pulsar)。
- 感谢存储引擎 [RocksDB](https://github.com/facebook/rocksdb)。
