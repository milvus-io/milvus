<img src="https://repository-images.githubusercontent.com/208728772/998c09ca-cfa6-4c01-ac75-3dfad7f4862b" alt="Milvus banner">

<div class="column" align="middle">
  <a href="https://milvusio.slack.com/join/shared_invite/zt-1slimkif6-8uWK0XPL8adve6vSD4jSwg">
    <img src="https://img.shields.io/badge/Join-Slack-orange" alt="join-slack"/>
  </a>
  <img src="https://img.shields.io/github/license/milvus-io/milvus" alt="license"/>
  <img src="https://img.shields.io/docker/pulls/milvusdb/milvus" alt="docker-pull-count" />
</div>

# 欢迎来到 Milvus

## Milvus 是什么

Milvus 是一款全球领先的开源向量数据库，赋能 AI 应用和向量相似度搜索，加速非结构化数据检索。用户在任何部署环境中均可获得始终如一的用户体验。

Milvus 2.0 是一款云原生向量数据库，采用存储与计算分离的架构设计。该重构版本的所有组件均为无状态组件，极大地增强了系统弹性和灵活性。更多系统架构细节，参考 [Milvus 系统架构](https://milvus.io/cn/docs/architecture_overview.md)。

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
  Milvus 项目在 GitHub 上获星超 8000，拥有逾 1000 家企业用户，还有活跃的开源社区。Milvus 由 <a href="https://lfaidata.foundation/">LF AI & Data 基金会</a> 背书，是该基金会的毕业项目。
  </details>

## 快速开始

### 启动 Milvus

- [安装 Milvus 单机版](https://milvus.io/cn/docs/v2.0.0/install_standalone-docker.md)

- [安装 Milvus 分布式版本](https://milvus.io/cn/docs/v2.0.0/install_cluster-docker.md)

### 源码编译 Milvus

请先安装相关依赖。

```
go: 1.18
cmake: >=3.18
gcc: 7.5
protobuf: >=3.7
```

克隆 Milvus 项目并编译。

```bash
# 克隆项目
$ git clone https://github.com/milvus-io/milvus.git

# 安装第三方依赖
$ cd milvus/
$ ./scripts/install_deps.sh

# 编译 Milvus 单机版
$ make milvus
```

获取更多内容，请阅读 [开发者文档](https://github.com/milvus-io/milvus/blob/master/DEVELOPMENT.md)。

> **注意** 主分支用于 Milvus v2.0 代码开发。Milvus v1.0 于 2021 年 3 月 9 日发布，是 Milvus 的首个长期支持（LTS）版本。如需使用 Milvus 1.0，请切换至 [1.0 分支](https://github.com/milvus-io/milvus/tree/1.0)。

## Milvus 2.0：功能增加、性能升级

详见 [Milvus 2.0 vs. 1.x](https://github.com/milvus-io/milvus/blob/master/milvus20vs1x.md) 。

## 入门指南

### 应用场景

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
      <a href="https://milvus.io/milvus-demos">以图搜图系统</a>
    </th>
    <th>
      <a href="https://milvus.io/milvus-demos">智能问答机器人</a>
    </th>
    <th>
      <a href="https://milvus.io/milvus-demos">分子式检索系统</a>
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
<img src="https://img.shields.io/badge/all--contributors-312-orange"><br>
<a href="https://github.com/0xflotus"><img src="https://avatars.githubusercontent.com/u/26602940?v=4" width="30px" /></a>
<a href="https://github.com/ABNER-1"><img src="https://avatars.githubusercontent.com/u/24547351?v=4" width="30px" /></a>
<a href="https://github.com/Accagain2014"><img src="https://avatars.githubusercontent.com/u/9635216?v=4" width="30px" /></a>
<a href="https://github.com/AliDotS"><img src="https://avatars.githubusercontent.com/u/33119433?v=4" width="30px" /></a>
<a href="https://github.com/AllenYu1987"><img src="https://avatars.githubusercontent.com/u/12489985?v=4" width="30px" /></a>
<a href="https://github.com/Anosh21"><img src="https://avatars.githubusercontent.com/u/90505226?v=4" width="30px" /></a>
<a href="https://github.com/AnthonyTsu1984"><img src="https://avatars.githubusercontent.com/u/115786031?v=4" width="30px" /></a>
<a href="https://github.com/Aredcap"><img src="https://avatars.githubusercontent.com/u/40494761?v=4" width="30px" /></a>
<a href="https://github.com/Arya0812"><img src="https://avatars.githubusercontent.com/u/114047052?v=4" width="30px" /></a>
<a href="https://github.com/BUPTAnderson"><img src="https://avatars.githubusercontent.com/u/13449703?v=4" width="30px" /></a>
<a href="https://github.com/Ben-Aaron-Bio-Rad"><img src="https://avatars.githubusercontent.com/u/54123439?v=4" width="30px" /></a>
<a href="https://github.com/Bennu-Li"><img src="https://avatars.githubusercontent.com/u/53458891?v=4" width="30px" /></a>
<a href="https://github.com/Biki-das"><img src="https://avatars.githubusercontent.com/u/72331432?v=4" width="30px" /></a>
<a href="https://github.com/BossZou"><img src="https://avatars.githubusercontent.com/u/40255591?v=4" width="30px" /></a>
<a href="https://github.com/CNLHC"><img src="https://avatars.githubusercontent.com/u/21005146?v=4" width="30px" /></a>
<a href="https://github.com/Chisdo"><img src="https://avatars.githubusercontent.com/u/36720318?v=4" width="30px" /></a>
<a href="https://github.com/ChunelFeng"><img src="https://avatars.githubusercontent.com/u/37905059?v=4" width="30px" /></a>
<a href="https://github.com/CodeInDreams"><img src="https://avatars.githubusercontent.com/u/17664279?v=4" width="30px" /></a>
<a href="https://github.com/Cupchen"><img src="https://avatars.githubusercontent.com/u/34762375?v=4" width="30px" /></a>
<a href="https://github.com/DanielHuang1983"><img src="https://avatars.githubusercontent.com/u/4417873?v=4" width="30px" /></a>
<a href="https://github.com/Deep1Shikha"><img src="https://avatars.githubusercontent.com/u/47516502?v=4" width="30px" /></a>
<a href="https://github.com/DerekChia"><img src="https://avatars.githubusercontent.com/u/1457728?v=4" width="30px" /></a>
<a href="https://github.com/DingQK"><img src="https://avatars.githubusercontent.com/u/58072531?v=4" width="30px" /></a>
<a href="https://github.com/DiptoChakrabarty"><img src="https://avatars.githubusercontent.com/u/45638240?v=4" width="30px" /></a>
<a href="https://github.com/Emma-Song"><img src="https://avatars.githubusercontent.com/u/64460989?v=4" width="30px" /></a>
<a href="https://github.com/EricStarer"><img src="https://avatars.githubusercontent.com/u/34002927?v=4" width="30px" /></a>
<a href="https://github.com/Erzangel"><img src="https://avatars.githubusercontent.com/u/57399897?v=4" width="30px" /></a>
<a href="https://github.com/Fierralin"><img src="https://avatars.githubusercontent.com/u/8857059?v=4" width="30px" /></a>
<a href="https://github.com/FluorineDog"><img src="https://avatars.githubusercontent.com/u/15663612?v=4" width="30px" /></a>
<a href="https://github.com/Giriraj-Roy"><img src="https://avatars.githubusercontent.com/u/88903134?v=4" width="30px" /></a>
<a href="https://github.com/Googleaa"><img src="https://avatars.githubusercontent.com/u/55842817?v=4" width="30px" /></a>
<a href="https://github.com/Gracieeea"><img src="https://avatars.githubusercontent.com/u/50101579?v=4" width="30px" /></a>
<a href="https://github.com/GuanyunFeng"><img src="https://avatars.githubusercontent.com/u/40229765?v=4" width="30px" /></a>
<a href="https://github.com/GuoRentong"><img src="https://avatars.githubusercontent.com/u/57477222?v=4" width="30px" /></a>
<a href="https://github.com/GuyKh"><img src="https://avatars.githubusercontent.com/u/3136012?v=4" width="30px" /></a>
<a href="https://github.com/Hard-Coder05"><img src="https://avatars.githubusercontent.com/u/54059881?v=4" width="30px" /></a>
<a href="https://github.com/Heisenberg-Y"><img src="https://avatars.githubusercontent.com/u/35055583?v=4" width="30px" /></a>
<a href="https://github.com/HesterG"><img src="https://avatars.githubusercontent.com/u/17645053?v=4" width="30px" /></a>
<a href="https://github.com/HuangHua"><img src="https://avatars.githubusercontent.com/u/2274405?v=4" width="30px" /></a>
<a href="https://github.com/JackLCL"><img src="https://avatars.githubusercontent.com/u/53512883?v=4" width="30px" /></a>
<a href="https://github.com/JadeFlute0127"><img src="https://avatars.githubusercontent.com/u/35321989?v=4" width="30px" /></a>
<a href="https://github.com/Javajava1"><img src="https://avatars.githubusercontent.com/u/29594737?v=4" width="30px" /></a>
<a href="https://github.com/JinHai-CN"><img src="https://avatars.githubusercontent.com/u/33142505?v=4" width="30px" /></a>
<a href="https://github.com/Juneezee"><img src="https://avatars.githubusercontent.com/u/20135478?v=4" width="30px" /></a>
<a href="https://github.com/KubrickLiu"><img src="https://avatars.githubusercontent.com/u/24795136?v=4" width="30px" /></a>
<a href="https://github.com/LeoReeYang"><img src="https://avatars.githubusercontent.com/u/58654486?v=4" width="30px" /></a>
<a href="https://github.com/Leslie-Wong-H"><img src="https://avatars.githubusercontent.com/u/27696701?v=4" width="30px" /></a>
<a href="https://github.com/Lin-gh-Saint"><img src="https://avatars.githubusercontent.com/u/64019322?v=4" width="30px" /></a>
<a href="https://github.com/LionelDong"><img src="https://avatars.githubusercontent.com/u/7533395?v=4" width="30px" /></a>
<a href="https://github.com/LocoRichard"><img src="https://avatars.githubusercontent.com/u/81553353?v=4" width="30px" /></a>
<a href="https://github.com/LoveEachDay"><img src="https://avatars.githubusercontent.com/u/1573213?v=4" width="30px" /></a>
<a href="https://github.com/MXDA"><img src="https://avatars.githubusercontent.com/u/47274057?v=4" width="30px" /></a>
<a href="https://github.com/MrPresent-Han"><img src="https://avatars.githubusercontent.com/u/116052805?v=4" width="30px" /></a>
<a href="https://github.com/NavanshGoel"><img src="https://avatars.githubusercontent.com/u/74401713?v=4" width="30px" /></a>
<a href="https://github.com/NicoYuan1986"><img src="https://avatars.githubusercontent.com/u/109071306?v=4" width="30px" /></a>
<a href="https://github.com/NotRyan"><img src="https://avatars.githubusercontent.com/u/5742796?v=4" width="30px" /></a>
<a href="https://github.com/PahudPlus"><img src="https://avatars.githubusercontent.com/u/64403786?v=4" width="30px" /></a>
<a href="https://github.com/PowderLi"><img src="https://avatars.githubusercontent.com/u/135960789?v=4" width="30px" /></a>
<a href="https://github.com/Presburger"><img src="https://avatars.githubusercontent.com/u/49336176?v=4" width="30px" /></a>
<a href="https://github.com/QipengZhou"><img src="https://avatars.githubusercontent.com/u/5410298?v=4" width="30px" /></a>
<a href="https://github.com/RafaelDSS"><img src="https://avatars.githubusercontent.com/u/27598602?v=4" width="30px" /></a>
<a href="https://github.com/RangerCD"><img src="https://avatars.githubusercontent.com/u/6872198?v=4" width="30px" /></a>
<a href="https://github.com/ReigenAraka"><img src="https://avatars.githubusercontent.com/u/57280231?v=4" width="30px" /></a>
<a href="https://github.com/RosieZhang12"><img src="https://avatars.githubusercontent.com/u/106942883?v=4" width="30px" /></a>
<a href="https://github.com/RyanWei"><img src="https://avatars.githubusercontent.com/u/9876551?v=4" width="30px" /></a>
<a href="https://github.com/SCKCZJ2018"><img src="https://avatars.githubusercontent.com/u/29282370?v=4" width="30px" /></a>
<a href="https://github.com/SarthakJain26"><img src="https://avatars.githubusercontent.com/u/45846277?v=4" width="30px" /></a>
<a href="https://github.com/SimFG"><img src="https://avatars.githubusercontent.com/u/21985684?v=4" width="30px" /></a>
<a href="https://github.com/SkyYang"><img src="https://avatars.githubusercontent.com/u/4702509?v=4" width="30px" /></a>
<a href="https://github.com/SnowyOwl-KHY"><img src="https://avatars.githubusercontent.com/u/10348819?v=4" width="30px" /></a>
<a href="https://github.com/Spnetic-5"><img src="https://avatars.githubusercontent.com/u/66636289?v=4" width="30px" /></a>
<a href="https://github.com/Sunt-ing"><img src="https://avatars.githubusercontent.com/u/43040147?v=4" width="30px" /></a>
<a href="https://github.com/SwaggySong"><img src="https://avatars.githubusercontent.com/u/36157116?v=4" width="30px" /></a>
<a href="https://github.com/Thor-ChenBiao"><img src="https://avatars.githubusercontent.com/u/104345188?v=4" width="30px" /></a>
<a href="https://github.com/ThreadDao"><img src="https://avatars.githubusercontent.com/u/27288593?v=4" width="30px" /></a>
<a href="https://github.com/ThyeeZz"><img src="https://avatars.githubusercontent.com/u/41352919?v=4" width="30px" /></a>
<a href="https://github.com/Tlincy"><img src="https://avatars.githubusercontent.com/u/11934432?v=4" width="30px" /></a>
<a href="https://github.com/Tumao727"><img src="https://avatars.githubusercontent.com/u/20420181?v=4" width="30px" /></a>
<a href="https://github.com/Writtic"><img src="https://avatars.githubusercontent.com/u/11371498?v=4" width="30px" /></a>
<a href="https://github.com/Xieql"><img src="https://avatars.githubusercontent.com/u/45359033?v=4" width="30px" /></a>
<a href="https://github.com/XuPeng-SH"><img src="https://avatars.githubusercontent.com/u/39627130?v=4" width="30px" /></a>
<a href="https://github.com/XuanYang-cn"><img src="https://avatars.githubusercontent.com/u/51370125?v=4" width="30px" /></a>
<a href="https://github.com/YiyunNi"><img src="https://avatars.githubusercontent.com/u/74396087?v=4" width="30px" /></a>
<a href="https://github.com/Yukikaze-CZR"><img src="https://avatars.githubusercontent.com/u/48198922?v=4" width="30px" /></a>
<a href="https://github.com/Zach41"><img src="https://avatars.githubusercontent.com/u/3941604?v=4" width="30px" /></a>
<a href="https://github.com/ZhaoBQ"><img src="https://avatars.githubusercontent.com/u/35092554?v=4" width="30px" /></a>
<a href="https://github.com/aakejiang"><img src="https://avatars.githubusercontent.com/u/68629395?v=4" width="30px" /></a>
<a href="https://github.com/aaronjin2010"><img src="https://avatars.githubusercontent.com/u/48044391?v=4" width="30px" /></a>
<a href="https://github.com/aharonh"><img src="https://avatars.githubusercontent.com/u/350928?v=4" width="30px" /></a>
<a href="https://github.com/akihoni"><img src="https://avatars.githubusercontent.com/u/36330442?v=4" width="30px" /></a>
<a href="https://github.com/alwayslove2013"><img src="https://avatars.githubusercontent.com/u/22510720?v=4" width="30px" /></a>
<a href="https://github.com/anchun"><img src="https://avatars.githubusercontent.com/u/2356895?v=4" width="30px" /></a>
<a href="https://github.com/aoiasd"><img src="https://avatars.githubusercontent.com/u/45024769?v=4" width="30px" /></a>
<a href="https://github.com/arijit-chowdhury-genea"><img src="https://avatars.githubusercontent.com/u/104769013?v=4" width="30px" /></a>
<a href="https://github.com/armanexplorer"><img src="https://avatars.githubusercontent.com/u/44166374?v=4" width="30px" /></a>
<a href="https://github.com/ashyshyshyman"><img src="https://avatars.githubusercontent.com/u/50362613?v=4" width="30px" /></a>
<a href="https://github.com/avats-dev"><img src="https://avatars.githubusercontent.com/u/35889327?v=4" width="30px" /></a>
<a href="https://github.com/avsolatorio"><img src="https://avatars.githubusercontent.com/u/3009596?v=4" width="30px" /></a>
<a href="https://github.com/become-nice"><img src="https://avatars.githubusercontent.com/u/56624819?v=4" width="30px" /></a>
<a href="https://github.com/bigsheeper"><img src="https://avatars.githubusercontent.com/u/42060877?v=4" width="30px" /></a>
<a href="https://github.com/binbin12580"><img src="https://avatars.githubusercontent.com/u/30914966?v=4" width="30px" /></a>
<a href="https://github.com/binbinlv"><img src="https://avatars.githubusercontent.com/u/83755740?v=4" width="30px" /></a>
<a href="https://github.com/bo-huang"><img src="https://avatars.githubusercontent.com/u/24309515?v=4" width="30px" /></a>
<a href="https://github.com/bryanwux"><img src="https://avatars.githubusercontent.com/u/17968665?v=4" width="30px" /></a>
<a href="https://github.com/caesarjuly"><img src="https://avatars.githubusercontent.com/u/927521?v=4" width="30px" /></a>
<a href="https://github.com/caosiyang"><img src="https://avatars.githubusercontent.com/u/2155120?v=4" width="30px" /></a>
<a href="https://github.com/carawaylj"><img src="https://avatars.githubusercontent.com/u/69145751?v=4" width="30px" /></a>
<a href="https://github.com/charlspjohn"><img src="https://avatars.githubusercontent.com/u/14850736?v=4" width="30px" /></a>
<a href="https://github.com/chasingegg"><img src="https://avatars.githubusercontent.com/u/18375889?v=4" width="30px" /></a>
<a href="https://github.com/chengpu"><img src="https://avatars.githubusercontent.com/u/2233492?v=4" width="30px" /></a>
<a href="https://github.com/chensanle"><img src="https://avatars.githubusercontent.com/u/31087327?v=4" width="30px" /></a>
<a href="https://github.com/chenxingqiang"><img src="https://avatars.githubusercontent.com/u/12387235?v=4" width="30px" /></a>
<a href="https://github.com/chinalu"><img src="https://avatars.githubusercontent.com/u/167307?v=4" width="30px" /></a>
<a href="https://github.com/chinamcafee"><img src="https://avatars.githubusercontent.com/u/3439961?v=4" width="30px" /></a>
<a href="https://github.com/chuangfengwang"><img src="https://avatars.githubusercontent.com/u/24692397?v=4" width="30px" /></a>
<a href="https://github.com/chyezh"><img src="https://avatars.githubusercontent.com/u/20332315?v=4" width="30px" /></a>
<a href="https://github.com/cjrh"><img src="https://avatars.githubusercontent.com/u/480395?v=4" width="30px" /></a>
<a href="https://github.com/claireyuw"><img src="https://avatars.githubusercontent.com/u/83751381?v=4" width="30px" /></a>
<a href="https://github.com/codacy-badger"><img src="https://avatars.githubusercontent.com/u/23704769?v=4" width="30px" /></a>
<a href="https://github.com/codenoid"><img src="https://avatars.githubusercontent.com/u/14269809?v=4" width="30px" /></a>
<a href="https://github.com/congqixia"><img src="https://avatars.githubusercontent.com/u/84113973?v=4" width="30px" /></a>
<a href="https://github.com/corest"><img src="https://avatars.githubusercontent.com/u/1071648?v=4" width="30px" /></a>
<a href="https://github.com/cqy123456"><img src="https://avatars.githubusercontent.com/u/39671710?v=4" width="30px" /></a>
<a href="https://github.com/cuishuang"><img src="https://avatars.githubusercontent.com/u/15921519?v=4" width="30px" /></a>
<a href="https://github.com/cxie"><img src="https://avatars.githubusercontent.com/u/653101?v=4" width="30px" /></a>
<a href="https://github.com/cxytz01"><img src="https://avatars.githubusercontent.com/u/18002438?v=4" width="30px" /></a>
<a href="https://github.com/cydrain"><img src="https://avatars.githubusercontent.com/u/3992404?v=4" width="30px" /></a>
<a href="https://github.com/czhen-zilliz"><img src="https://avatars.githubusercontent.com/u/83751452?v=4" width="30px" /></a>
<a href="https://github.com/czpmango"><img src="https://avatars.githubusercontent.com/u/26356194?v=4" width="30px" /></a>
<a href="https://github.com/czs007"><img src="https://avatars.githubusercontent.com/u/59249785?v=4" width="30px" /></a>
<a href="https://github.com/dariocurr"><img src="https://avatars.githubusercontent.com/u/48800335?v=4" width="30px" /></a>
<a href="https://github.com/datenhahn"><img src="https://avatars.githubusercontent.com/u/13999666?v=4" width="30px" /></a>
<a href="https://github.com/dd-He"><img src="https://avatars.githubusercontent.com/u/24242249?v=4" width="30px" /></a>
<a href="https://github.com/dddddai"><img src="https://avatars.githubusercontent.com/u/41563853?v=4" width="30px" /></a>
<a href="https://github.com/del-zhenwu"><img src="https://avatars.githubusercontent.com/u/56623710?v=4" width="30px" /></a>
<a href="https://github.com/donno2048"><img src="https://avatars.githubusercontent.com/u/61805754?v=4" width="30px" /></a>
<a href="https://github.com/douglarek"><img src="https://avatars.githubusercontent.com/u/1488134?v=4" width="30px" /></a>
<a href="https://github.com/drow931"><img src="https://avatars.githubusercontent.com/u/11514434?v=4" width="30px" /></a>
<a href="https://github.com/dvzubarev"><img src="https://avatars.githubusercontent.com/u/14878830?v=4" width="30px" /></a>
<a href="https://github.com/dyhyfu"><img src="https://avatars.githubusercontent.com/u/64584368?v=4" width="30px" /></a>
<a href="https://github.com/egoebelbecker"><img src="https://avatars.githubusercontent.com/u/5241455?v=4" width="30px" /></a>
<a href="https://github.com/elfisworking"><img src="https://avatars.githubusercontent.com/u/37609214?v=4" width="30px" /></a>
<a href="https://github.com/eliassama"><img src="https://avatars.githubusercontent.com/u/79587688?v=4" width="30px" /></a>
<a href="https://github.com/elstic"><img src="https://avatars.githubusercontent.com/u/48523564?v=4" width="30px" /></a>
<a href="https://github.com/eltociear"><img src="https://avatars.githubusercontent.com/u/22633385?v=4" width="30px" /></a>
<a href="https://github.com/erdustiggen"><img src="https://avatars.githubusercontent.com/u/25433850?v=4" width="30px" /></a>
<a href="https://github.com/feisiyicl"><img src="https://avatars.githubusercontent.com/u/64510805?v=4" width="30px" /></a>
<a href="https://github.com/filip-halt"><img src="https://avatars.githubusercontent.com/u/81822489?v=4" width="30px" /></a>
<a href="https://github.com/filipecaixeta"><img src="https://avatars.githubusercontent.com/u/1094052?v=4" width="30px" /></a>
<a href="https://github.com/fishpenguin"><img src="https://avatars.githubusercontent.com/u/49153041?v=4" width="30px" /></a>
<a href="https://github.com/foxspy"><img src="https://avatars.githubusercontent.com/u/11503321?v=4" width="30px" /></a>
<a href="https://github.com/freestsoul"><img src="https://avatars.githubusercontent.com/u/3909908?v=4" width="30px" /></a>
<a href="https://github.com/ggaaooppeenngg"><img src="https://avatars.githubusercontent.com/u/4769989?v=4" width="30px" /></a>
<a href="https://github.com/git-hulk"><img src="https://avatars.githubusercontent.com/u/4987594?v=4" width="30px" /></a>
<a href="https://github.com/godchen0212"><img src="https://avatars.githubusercontent.com/u/67679556?v=4" width="30px" /></a>
<a href="https://github.com/goodhamgupta"><img src="https://avatars.githubusercontent.com/u/14368181?v=4" width="30px" /></a>
<a href="https://github.com/gouzil"><img src="https://avatars.githubusercontent.com/u/66515297?v=4" width="30px" /></a>
<a href="https://github.com/gracezzzzz"><img src="https://avatars.githubusercontent.com/u/56617657?v=4" width="30px" /></a>
<a href="https://github.com/grtoverflow"><img src="https://avatars.githubusercontent.com/u/8500564?v=4" width="30px" /></a>
<a href="https://github.com/gruebel"><img src="https://avatars.githubusercontent.com/u/33207684?v=4" width="30px" /></a>
<a href="https://github.com/gujun720"><img src="https://avatars.githubusercontent.com/u/53246671?v=4" width="30px" /></a>
<a href="https://github.com/guoxiangzhou"><img src="https://avatars.githubusercontent.com/u/52496626?v=4" width="30px" /></a>
<a href="https://github.com/hadim"><img src="https://avatars.githubusercontent.com/u/528003?v=4" width="30px" /></a>
<a href="https://github.com/haorenfsa"><img src="https://avatars.githubusercontent.com/u/15938850?v=4" width="30px" /></a>
<a href="https://github.com/hey-hoho"><img src="https://avatars.githubusercontent.com/u/8401517?v=4" width="30px" /></a>
<a href="https://github.com/huanghaoyuanhhy"><img src="https://avatars.githubusercontent.com/u/103482615?v=4" width="30px" /></a>
<a href="https://github.com/huangjincheng2022"><img src="https://avatars.githubusercontent.com/u/98305308?v=4" width="30px" /></a>
<a href="https://github.com/ibrahimhaddad"><img src="https://avatars.githubusercontent.com/u/1656002?v=4" width="30px" /></a>
<a href="https://github.com/im-ajaymeena"><img src="https://avatars.githubusercontent.com/u/19550841?v=4" width="30px" /></a>
<a href="https://github.com/ireneontheway5"><img src="https://avatars.githubusercontent.com/u/75291211?v=4" width="30px" /></a>
<a href="https://github.com/iynewz"><img src="https://avatars.githubusercontent.com/u/81401074?v=4" width="30px" /></a>
<a href="https://github.com/izapolsk"><img src="https://avatars.githubusercontent.com/u/21039333?v=4" width="30px" /></a>
<a href="https://github.com/jackyu2020"><img src="https://avatars.githubusercontent.com/u/64533877?v=4" width="30px" /></a>
<a href="https://github.com/jaelgu"><img src="https://avatars.githubusercontent.com/u/86251631?v=4" width="30px" /></a>
<a href="https://github.com/jaime0815"><img src="https://avatars.githubusercontent.com/u/4024711?v=4" width="30px" /></a>
<a href="https://github.com/jeffoverflow"><img src="https://avatars.githubusercontent.com/u/24581746?v=4" width="30px" /></a>
<a href="https://github.com/jeffreye"><img src="https://avatars.githubusercontent.com/u/1737680?v=4" width="30px" /></a>
<a href="https://github.com/jennyli-z"><img src="https://avatars.githubusercontent.com/u/93511422?v=4" width="30px" /></a>
<a href="https://github.com/jiaoew1991"><img src="https://avatars.githubusercontent.com/u/2297455?v=4" width="30px" /></a>
<a href="https://github.com/jielinxu"><img src="https://avatars.githubusercontent.com/u/52057195?v=4" width="30px" /></a>
<a href="https://github.com/jingkl"><img src="https://avatars.githubusercontent.com/u/34296482?v=4" width="30px" /></a>
<a href="https://github.com/jjyaoao"><img src="https://avatars.githubusercontent.com/u/88936287?v=4" width="30px" /></a>
<a href="https://github.com/jkx8fc"><img src="https://avatars.githubusercontent.com/u/31717785?v=4" width="30px" /></a>
<a href="https://github.com/john-h-luo"><img src="https://avatars.githubusercontent.com/u/67673717?v=4" width="30px" /></a>
<a href="https://github.com/kartikcho"><img src="https://avatars.githubusercontent.com/u/48270786?v=4" width="30px" /></a>
<a href="https://github.com/kateshaowanjou"><img src="https://avatars.githubusercontent.com/u/58837504?v=4" width="30px" /></a>
<a href="https://github.com/kilianovski"><img src="https://avatars.githubusercontent.com/u/22048793?v=4" width="30px" /></a>
<a href="https://github.com/klboke"><img src="https://avatars.githubusercontent.com/u/18591662?v=4" width="30px" /></a>
<a href="https://github.com/lee-eve"><img src="https://avatars.githubusercontent.com/u/9720105?v=4" width="30px" /></a>
<a href="https://github.com/leonardokidd"><img src="https://avatars.githubusercontent.com/u/14940941?v=4" width="30px" /></a>
<a href="https://github.com/letian-jiang"><img src="https://avatars.githubusercontent.com/u/16740944?v=4" width="30px" /></a>
<a href="https://github.com/leykun10"><img src="https://avatars.githubusercontent.com/u/45382760?v=4" width="30px" /></a>
<a href="https://github.com/lhotari"><img src="https://avatars.githubusercontent.com/u/66864?v=4" width="30px" /></a>
<a href="https://github.com/liliu-z"><img src="https://avatars.githubusercontent.com/u/105927039?v=4" width="30px" /></a>
<a href="https://github.com/linchpinlin"><img src="https://avatars.githubusercontent.com/u/31131753?v=4" width="30px" /></a>
<a href="https://github.com/linhgao"><img src="https://avatars.githubusercontent.com/u/102851605?v=4" width="30px" /></a>
<a href="https://github.com/lixiangMindSpore"><img src="https://avatars.githubusercontent.com/u/78945582?v=4" width="30px" /></a>
<a href="https://github.com/liyun95"><img src="https://avatars.githubusercontent.com/u/105278390?v=4" width="30px" /></a>
<a href="https://github.com/locustbaby"><img src="https://avatars.githubusercontent.com/u/21237232?v=4" width="30px" /></a>
<a href="https://github.com/loguo"><img src="https://avatars.githubusercontent.com/u/15364733?v=4" width="30px" /></a>
<a href="https://github.com/longjiquan"><img src="https://avatars.githubusercontent.com/u/31589260?v=4" width="30px" /></a>
<a href="https://github.com/lsgrep"><img src="https://avatars.githubusercontent.com/u/3893940?v=4" width="30px" /></a>
<a href="https://github.com/lwglgy"><img src="https://avatars.githubusercontent.com/u/26682620?v=4" width="30px" /></a>
<a href="https://github.com/maclandrol"><img src="https://avatars.githubusercontent.com/u/5290110?v=4" width="30px" /></a>
<a href="https://github.com/maksspace"><img src="https://avatars.githubusercontent.com/u/9841409?v=4" width="30px" /></a>
<a href="https://github.com/matchyc"><img src="https://avatars.githubusercontent.com/u/57976772?v=4" width="30px" /></a>
<a href="https://github.com/matrixji"><img src="https://avatars.githubusercontent.com/u/183388?v=4" width="30px" /></a>
<a href="https://github.com/mausch"><img src="https://avatars.githubusercontent.com/u/95194?v=4" width="30px" /></a>
<a href="https://github.com/miia12"><img src="https://avatars.githubusercontent.com/u/22544815?v=4" width="30px" /></a>
<a href="https://github.com/mileyzjq"><img src="https://avatars.githubusercontent.com/u/37039827?v=4" width="30px" /></a>
<a href="https://github.com/milvus-ci-robot"><img src="https://avatars.githubusercontent.com/u/87847967?v=4" width="30px" /></a>
<a href="https://github.com/moe-of-faith"><img src="https://avatars.githubusercontent.com/u/5696721?v=4" width="30px" /></a>
<a href="https://github.com/mohitreddy1996"><img src="https://avatars.githubusercontent.com/u/11742913?v=4" width="30px" /></a>
<a href="https://github.com/nameczz"><img src="https://avatars.githubusercontent.com/u/20559208?v=4" width="30px" /></a>
<a href="https://github.com/natoka"><img src="https://avatars.githubusercontent.com/u/1751024?v=4" width="30px" /></a>
<a href="https://github.com/nexttonever"><img src="https://avatars.githubusercontent.com/u/31059690?v=4" width="30px" /></a>
<a href="https://github.com/neza2017"><img src="https://avatars.githubusercontent.com/u/34152706?v=4" width="30px" /></a>
<a href="https://github.com/op-hunter"><img src="https://avatars.githubusercontent.com/u/5617677?v=4" width="30px" /></a>
<a href="https://github.com/ownbylichaobao"><img src="https://avatars.githubusercontent.com/u/37684963?v=4" width="30px" /></a>
<a href="https://github.com/panjf2000"><img src="https://avatars.githubusercontent.com/u/7496278?v=4" width="30px" /></a>
<a href="https://github.com/parsa-ra"><img src="https://avatars.githubusercontent.com/u/11356471?v=4" width="30px" /></a>
<a href="https://github.com/pengjeck"><img src="https://avatars.githubusercontent.com/u/14035577?v=4" width="30px" /></a>
<a href="https://github.com/phantom8548"><img src="https://avatars.githubusercontent.com/u/11576622?v=4" width="30px" /></a>
<a href="https://github.com/preetham"><img src="https://avatars.githubusercontent.com/u/9149028?v=4" width="30px" /></a>
<a href="https://github.com/psc0606"><img src="https://avatars.githubusercontent.com/u/7888889?v=4" width="30px" /></a>
<a href="https://github.com/punkerpunker"><img src="https://avatars.githubusercontent.com/u/54440025?v=4" width="30px" /></a>
<a href="https://github.com/qbzenker"><img src="https://avatars.githubusercontent.com/u/51972064?v=4" width="30px" /></a>
<a href="https://github.com/rahulmistri1997"><img src="https://avatars.githubusercontent.com/u/58909377?v=4" width="30px" /></a>
<a href="https://github.com/ronnie-llamado"><img src="https://avatars.githubusercontent.com/u/35092029?v=4" width="30px" /></a>
<a href="https://github.com/saisona"><img src="https://avatars.githubusercontent.com/u/10884762?v=4" width="30px" /></a>
<a href="https://github.com/sapora1"><img src="https://avatars.githubusercontent.com/u/59124772?v=4" width="30px" /></a>
<a href="https://github.com/scipe"><img src="https://avatars.githubusercontent.com/u/3996622?v=4" width="30px" /></a>
<a href="https://github.com/scsven"><img src="https://avatars.githubusercontent.com/u/100122127?v=4" width="30px" /></a>
<a href="https://github.com/seo-jinBro"><img src="https://avatars.githubusercontent.com/u/17746814?v=4" width="30px" /></a>
<a href="https://github.com/septemberfd"><img src="https://avatars.githubusercontent.com/u/40378371?v=4" width="30px" /></a>
<a href="https://github.com/shana0325"><img src="https://avatars.githubusercontent.com/u/33335490?v=4" width="30px" /></a>
<a href="https://github.com/shanghaikid"><img src="https://avatars.githubusercontent.com/u/185051?v=4" width="30px" /></a>
<a href="https://github.com/shengjh"><img src="https://avatars.githubusercontent.com/u/46514371?v=4" width="30px" /></a>
<a href="https://github.com/shengjun1985"><img src="https://avatars.githubusercontent.com/u/49774184?v=4" width="30px" /></a>
<a href="https://github.com/shiyu09"><img src="https://avatars.githubusercontent.com/u/39143280?v=4" width="30px" /></a>
<a href="https://github.com/shiyu22"><img src="https://avatars.githubusercontent.com/u/53459423?v=4" width="30px" /></a>
<a href="https://github.com/shunjiezhao"><img src="https://avatars.githubusercontent.com/u/90906581?v=4" width="30px" /></a>
<a href="https://github.com/sileht"><img src="https://avatars.githubusercontent.com/u/200878?v=4" width="30px" /></a>
<a href="https://github.com/siriusctrl"><img src="https://avatars.githubusercontent.com/u/26541600?v=4" width="30px" /></a>
<a href="https://github.com/smellthemoon"><img src="https://avatars.githubusercontent.com/u/64083300?v=4" width="30px" /></a>
<a href="https://github.com/snewcomer"><img src="https://avatars.githubusercontent.com/u/7374640?v=4" width="30px" /></a>
<a href="https://github.com/snyk-bot"><img src="https://avatars.githubusercontent.com/u/19733683?v=4" width="30px" /></a>
<a href="https://github.com/songxianj"><img src="https://avatars.githubusercontent.com/u/107831450?v=4" width="30px" /></a>
<a href="https://github.com/soothing-rain"><img src="https://avatars.githubusercontent.com/u/69466447?v=4" width="30px" /></a>
<a href="https://github.com/soulteary"><img src="https://avatars.githubusercontent.com/u/1500781?v=4" width="30px" /></a>
<a href="https://github.com/sre-ci-robot"><img src="https://avatars.githubusercontent.com/u/56469371?v=4" width="30px" /></a>
<a href="https://github.com/sre-ro"><img src="https://avatars.githubusercontent.com/u/93502486?v=4" width="30px" /></a>
<a href="https://github.com/sreyan-ghosh"><img src="https://avatars.githubusercontent.com/u/60854658?v=4" width="30px" /></a>
<a href="https://github.com/ss892714028"><img src="https://avatars.githubusercontent.com/u/34635663?v=4" width="30px" /></a>
<a href="https://github.com/stuartjing"><img src="https://avatars.githubusercontent.com/u/3454260?v=4" width="30px" /></a>
<a href="https://github.com/sty945"><img src="https://avatars.githubusercontent.com/u/10708326?v=4" width="30px" /></a>
<a href="https://github.com/sunby"><img src="https://avatars.githubusercontent.com/u/9817127?v=4" width="30px" /></a>
<a href="https://github.com/sutcalag"><img src="https://avatars.githubusercontent.com/u/83750738?v=4" width="30px" /></a>
<a href="https://github.com/sworddish"><img src="https://avatars.githubusercontent.com/u/219938?v=4" width="30px" /></a>
<a href="https://github.com/talentAN"><img src="https://avatars.githubusercontent.com/u/17634030?v=4" width="30px" /></a>
<a href="https://github.com/taydy"><img src="https://avatars.githubusercontent.com/u/24822588?v=4" width="30px" /></a>
<a href="https://github.com/tbickford"><img src="https://avatars.githubusercontent.com/u/814232?v=4" width="30px" /></a>
<a href="https://github.com/tedyu"><img src="https://avatars.githubusercontent.com/u/235188?v=4" width="30px" /></a>
<a href="https://github.com/thywdy"><img src="https://avatars.githubusercontent.com/u/56624359?v=4" width="30px" /></a>
<a href="https://github.com/tinkerlin"><img src="https://avatars.githubusercontent.com/u/13817362?v=4" width="30px" /></a>
<a href="https://github.com/trovwu"><img src="https://avatars.githubusercontent.com/u/89676996?v=4" width="30px" /></a>
<a href="https://github.com/wangting0128"><img src="https://avatars.githubusercontent.com/u/26307815?v=4" width="30px" /></a>
<a href="https://github.com/want-fly"><img src="https://avatars.githubusercontent.com/u/36727480?v=4" width="30px" /></a>
<a href="https://github.com/water32"><img src="https://avatars.githubusercontent.com/u/13234561?v=4" width="30px" /></a>
<a href="https://github.com/wayblink"><img src="https://avatars.githubusercontent.com/u/18096561?v=4" width="30px" /></a>
<a href="https://github.com/weiZhenkun"><img src="https://avatars.githubusercontent.com/u/27683687?v=4" width="30px" /></a>
<a href="https://github.com/weiliu1031"><img src="https://avatars.githubusercontent.com/u/108661493?v=4" width="30px" /></a>
<a href="https://github.com/weishuo2"><img src="https://avatars.githubusercontent.com/u/27938020?v=4" width="30px" /></a>
<a href="https://github.com/wg1026688210"><img src="https://avatars.githubusercontent.com/u/14267759?v=4" width="30px" /></a>
<a href="https://github.com/wh201906"><img src="https://avatars.githubusercontent.com/u/62299611?v=4" width="30px" /></a>
<a href="https://github.com/wscxyey"><img src="https://avatars.githubusercontent.com/u/48882296?v=4" width="30px" /></a>
<a href="https://github.com/wxyucs"><img src="https://avatars.githubusercontent.com/u/12595343?v=4" width="30px" /></a>
<a href="https://github.com/wxywb"><img src="https://avatars.githubusercontent.com/u/5432721?v=4" width="30px" /></a>
<a href="https://github.com/wzymumon"><img src="https://avatars.githubusercontent.com/u/46886508?v=4" width="30px" /></a>
<a href="https://github.com/xaxys"><img src="https://avatars.githubusercontent.com/u/28949072?v=4" width="30px" /></a>
<a href="https://github.com/xiangzhouguo"><img src="https://avatars.githubusercontent.com/u/93316470?v=4" width="30px" /></a>
<a href="https://github.com/xiaocai2333"><img src="https://avatars.githubusercontent.com/u/46207236?v=4" width="30px" /></a>
<a href="https://github.com/xiaofan-luan"><img src="https://avatars.githubusercontent.com/u/83447078?v=4" width="30px" /></a>
<a href="https://github.com/xiaohu4313888"><img src="https://avatars.githubusercontent.com/u/39088547?v=4" width="30px" /></a>
<a href="https://github.com/xige-16"><img src="https://avatars.githubusercontent.com/u/20124155?v=4" width="30px" /></a>
<a href="https://github.com/xiyichan"><img src="https://avatars.githubusercontent.com/u/34647972?v=4" width="30px" /></a>
<a href="https://github.com/xudalin0609"><img src="https://avatars.githubusercontent.com/u/35444753?v=4" width="30px" /></a>
<a href="https://github.com/xuqiwe"><img src="https://avatars.githubusercontent.com/u/57252655?v=4" width="30px" /></a>
<a href="https://github.com/yah01"><img src="https://avatars.githubusercontent.com/u/12216890?v=4" width="30px" /></a>
<a href="https://github.com/yamasite"><img src="https://avatars.githubusercontent.com/u/10089260?v=4" width="30px" /></a>
<a href="https://github.com/yanliang567"><img src="https://avatars.githubusercontent.com/u/82361606?v=4" width="30px" /></a>
<a href="https://github.com/yelusion2"><img src="https://avatars.githubusercontent.com/u/97278661?v=4" width="30px" /></a>
<a href="https://github.com/yhmo"><img src="https://avatars.githubusercontent.com/u/2282099?v=4" width="30px" /></a>
<a href="https://github.com/yiuluchen"><img src="https://avatars.githubusercontent.com/u/23047684?v=4" width="30px" /></a>
<a href="https://github.com/yiwangdr"><img src="https://avatars.githubusercontent.com/u/80064917?v=4" width="30px" /></a>
<a href="https://github.com/yongpengli-z"><img src="https://avatars.githubusercontent.com/u/103410837?v=4" width="30px" /></a>
<a href="https://github.com/youny626"><img src="https://avatars.githubusercontent.com/u/9016120?v=4" width="30px" /></a>
<a href="https://github.com/ytang07"><img src="https://avatars.githubusercontent.com/u/17556662?v=4" width="30px" /></a>
<a href="https://github.com/yxm1536"><img src="https://avatars.githubusercontent.com/u/62009483?v=4" width="30px" /></a>
<a href="https://github.com/yylstudy"><img src="https://avatars.githubusercontent.com/u/26402953?v=4" width="30px" /></a>
<a href="https://github.com/zamanmub"><img src="https://avatars.githubusercontent.com/u/32416908?v=4" width="30px" /></a>
<a href="https://github.com/zc2638"><img src="https://avatars.githubusercontent.com/u/28284116?v=4" width="30px" /></a>
<a href="https://github.com/zengxy"><img src="https://avatars.githubusercontent.com/u/11961641?v=4" width="30px" /></a>
<a href="https://github.com/zerowe-seven"><img src="https://avatars.githubusercontent.com/u/57790060?v=4" width="30px" /></a>
<a href="https://github.com/zhagnlu"><img src="https://avatars.githubusercontent.com/u/11935707?v=4" width="30px" /></a>
<a href="https://github.com/zhang787jun"><img src="https://avatars.githubusercontent.com/u/51014996?v=4" width="30px" /></a>
<a href="https://github.com/zhenwu-cn"><img src="https://avatars.githubusercontent.com/u/2993941?v=4" width="30px" /></a>
<a href="https://github.com/zhoubo0317"><img src="https://avatars.githubusercontent.com/u/51948620?v=4" width="30px" /></a>
<a href="https://github.com/zhuwenxing"><img src="https://avatars.githubusercontent.com/u/12268675?v=4" width="30px" /></a>
<a href="https://github.com/zhuyaguang"><img src="https://avatars.githubusercontent.com/u/8857976?v=4" width="30px" /></a>
<a href="https://github.com/zwd1208"><img src="https://avatars.githubusercontent.com/u/15153901?v=4" width="30px" /></a>
<a href="https://github.com/zxf2017"><img src="https://avatars.githubusercontent.com/u/29620478?v=4" width="30px" /></a>
<!-- Do not remove end of hero-bot --><br>

## Milvus 文档

获取更多有关安装、开发、部署和管理的指南，请查看 [Milvus 文档](https://milvus.io/cn/docs).

### SDK

- [PyMilvus](https://github.com/milvus-io/pymilvus)
- [Java SDK](https://github.com/milvus-io/milvus-sdk-java)(开发中)
- [Go SDK](https://github.com/milvus-io/milvus-sdk-go)(开发中)
- [Cpp SDK](https://github.com/milvus-io/milvus-sdk-cpp)(开发中)
- [Node SDK](https://github.com/milvus-io/milvus-sdk-node)

### Attu

Attu 提供了好用的图形化界面，帮助您更好的管理数据和Milvus.
- [快速开始](https://github.com/zilliztech/milvus-insight#quick-start)

## 社区

欢迎加入 [Slack](https://milvusio.slack.com/join/shared_invite/zt-1slimkif6-8uWK0XPL8adve6vSD4jSwg) 频道分享你的建议与问题。你也可以通过 [FAQ](https://milvus.io/cn/docs/performance_faq.md) 页面，查看常见问题及解答。

订阅 Milvus 邮件：

- [Technical Steering Committee](https://lists.lfai.foundation/g/milvus-tsc)
- [Technical Discussions](https://lists.lfai.foundation/g/milvus-technical-discuss)
- [Announcement](https://lists.lfai.foundation/g/milvus-announce)

关注 Milvus 社交媒体：

- [知乎](https://www.zhihu.com/org/zilliz-11)
- [CSDN](http://zilliz.blog.csdn.net/)
- [Bilibili](http://space.bilibili.com/478166626)
- [Medium](https://medium.com/@milvusio)
- [Twitter](https://twitter.com/milvusio)
- [Youtube](https://www.youtube.com/channel/UCMCo_F7pKjMHBlfyxwOPw-g)
- Zilliz 技术交流微信群

<img src="https://assets.zilliz.com/wechat_2abac21f5a.png" alt="Wechat QR Code">

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
