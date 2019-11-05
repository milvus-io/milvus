![Milvuslogo](https://github.com/milvus-io/docs/blob/master/assets/milvus_logo.png)


![LICENSE](https://img.shields.io/badge/license-Apache--2.0-brightgreen)
![Language](https://img.shields.io/badge/language-C%2B%2B-blue)
[![codebeat badge](https://codebeat.co/badges/e030a4f6-b126-4475-a938-4723d54ec3a7?style=plastic)](https://codebeat.co/projects/github-com-jinhai-cn-milvus-master)

- [Slack Community](https://join.slack.com/t/milvusio/shared_invite/enQtNzY1OTQ0NDI3NjMzLWNmYmM1NmNjOTQ5MGI5NDhhYmRhMGU5M2NhNzhhMDMzY2MzNDdlYjM5ODQ5MmE3ODFlYzU3YjJkNmVlNDQ2ZTk)
- [Twitter](https://twitter.com/milvusio)
- [Facebook](https://www.facebook.com/io.milvus.5)
- [Blog](https://www.milvus.io/blog/)
- [CSDN](https://zilliz.blog.csdn.net/)
- [中文官网](https://www.milvus.io/zh-CN/)


# Milvus へようこそ

## 概要

Milvus は大規模な特徴ベクトルにむかう類似性検索エンジンです。不均質な計算アーキテクチャーに基づいて効率を最大化出来ます。数十億のベクタの中に目標を検索できるまで数ミリ秒しかかからず、最低限の計算資源だけが必要です。

Milvus は安定的な Python、Java 又は C++ APIsを提供します。

Milvus [リリースノート](https://milvus.io/docs/en/release/v0.5.1/)を読んで最新バージョンや更新情報を手に入れます。

- 不均質な計算アーキテクチャー

  Milvusは不均質な計算アーキテクチャーに基づいて効率を最大化出来ます。

- 様々な索引方法

  Milvusはさまざまな索引方法を支えます。量子化、 木、 グラフに基づいて索引を作成できます。

- 知能てきな資源管理

  Milvusはデータセットと利用できる資源を基づいて、自動的に検索アルゴリズムと索引作成方法を選びます。

- 水平拡張

  Milvusはオンラインとオフラインに記憶域と計算を簡単な命令で拡張できます。

- 高い可用性

  MilvusはKubernetes枠組みと統合するので、 単一障害点を避けられます。

- 高い互換性

  Milvusはほぼ全ての深層学習モデルと主要なプログラミング言語と互換性があります。

- やすい使い方

  Milvusは簡単にインストールできます。ベクタ以外のことを心配する必要がありますん。

- 視覚化てきな監視モード

  Prometheusに基づいてGUIでシステムの性能を監視出来ます。

## アーキテクチャー

![Milvus_arch](https://github.com/milvus-io/docs/blob/master/assets/milvus_arch.png)

## はじめに

### ハードウェア要件

| コンポーネント | お勧めの配置           |
| --------- | ----------------------------------- |
| CPU       | Intel CPU Haswell 以上      |
| GPU       | NVIDIA Pascal series 以上   |
| RAM メモリ   | 8 GB 以上 (データ規模に関わる) |
| ハードディスク   | SATA 3.0 SSD 以上           |

### Dockerでインストールする

DockerでMilvusをインストールすることは簡単です。 [Milvusインストール案内](https://milvus.io/docs/en/userguide/install_milvus/) を参考してください。

### ソースから構築する

#### ソフトウェア要件

- Ubuntu 18.04 以上
- CMake 3.14 以上
- CUDA 10.0 以上
- NVIDIA driver 418 以上

#### コンパイル

##### 1  依存コンポーネントをインストールする

```shell
$ cd [Milvus sourcecode path]/core
./ubuntu_build_deps.sh
```

##### 2 構築する

```shell
$ cd [Milvus sourcecode path]/core
$ ./build.sh -t Debug
or 
$ ./build.sh -t Release
```

構築が完成するとき、 Milvusを実行するために必要なものは全てこのディレクトリにあります： `[Milvus root path]/core/milvus`。

#### Milvusサーバーを実行する

```shell
$ cd [Milvus root path]/core/milvus
```

`lib/`　ディレクトリを　`LD_LIBRARY_PATH`　に添付する。

```shell
$ export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/path/to/milvus/lib
```

Milvusサーバーを実行する。

```shell
$ cd scripts
$ ./start_server.sh
```

Milvusサーバーを止めるために、次のコードを実行する：

```shell
$ ./stop_server.sh
```

`conf/server_config.yaml` と `conf/log_config.conf` でMilvusをコンフィグするために、 [Milvusコンフィグ](https://github.com/milvus-io/docs/blob/master/reference/milvus_config.md)を読んでください。

### 初めてのMilvusプログラムを実行する

#### Pythonサンプルコードを実行する

[Python 3.5](https://www.python.org/downloads/)以上のバージョンがインストールされていることを確かめてください。

Milvus Python SDK　をインストールする。

```shell
# Install Milvus Python SDK
$ pip install pymilvus==0.2.3
```

新しいファイル　`example.py`　を作成し、 [Pythonサンプルコード]( https://github.com/milvus-io/pymilvus/blob/master/examples/advanced_example.py)を添付してください。

サンプルコードを実行する。

```shell
# Run Milvus Python example
$ python3 example.py
```

#### C++サンプルコードを実行する

```shell
 # Run Milvus C++ example
 $ cd [Milvus root path]/core/milvus/bin
 $ ./sdk_simple
```

#### Javaサンプルコードを実行する

Java 8以上のバージョンがインストールされていることを確かめてください。

[このリンク](https://github.com/milvus-io/milvus-sdk-java/tree/master/examples)でサンプルコードを手に入れます。

## 貢献規約

本プロジェクトへの貢献に心より感謝いたします。 Milvusを貢献したいと思うなら、[貢献規約](CONTRIBUTING.md)を読んでください。 本プロジェクトはMilvusの[行動規範](CODE_OF_CONDUCT.md)に従います。プロジェクトに参加したい場合は、行動規範を従ってください。

[GitHub issues](https://github.com/milvus-io/milvus/issues/new/choose) を使って問題やバッグなとを報告しでください。 一般てきな問題なら, Milvusコミュニティに参加してください。

## Milvusコミュニティを参加する

他の貢献者と交流したい場合は、Milvusの [slackチャンネル](https://join.slack.com/t/milvusio/shared_invite/enQtNzY1OTQ0NDI3NjMzLWNmYmM1NmNjOTQ5MGI5NDhhYmRhMGU5M2NhNzhhMDMzY2MzNDdlYjM5ODQ5MmE3ODFlYzU3YjJkNmVlNDQ2ZTk)に参加してください。

## Milvusロードマップ

[ロードマップ](https://milvus.io/docs/en/roadmap/)を読んで、追加する予定の特性が分かります。

## 参考情報

[Milvus公式サイト](https://www.milvus.io)

[Milvusドキュメント](https://www.milvus.io/docs/en/userguide/install_milvus/)

[Milvusトレーニングセンター](https://github.com/milvus-io/bootcamp)

[Milvusブロック](https://www.milvus.io/blog/)

[Milvus CSDN](https://zilliz.blog.csdn.net/)

[Milvusロードマップ](https://milvus.io/docs/en/roadmap/)


## ライセンス

[Apache 2.0ライセンス](LICENSE)
