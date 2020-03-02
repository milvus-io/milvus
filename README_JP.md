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
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/c4bb2ccfb51b47f99e43bfd1705edd95)](https://app.codacy.com/gh/milvus-io/milvus?utm_source=github.com&utm_medium=referral&utm_content=milvus-io/milvus&utm_campaign=Badge_Grade_Dashboard)
[![Scrutinizer Quality Score](https://scrutinizer-ci.com/g/milvus-io/milvus/badges/quality-score.png?b=master)](https://scrutinizer-ci.com/g/milvus-io/milvus/)

[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg?style=flat-square)](http://makeapullrequest.com)


# Milvus へようこそ

## 概要

Milvusは特徴ベクトルにむかうオーペンソース高性能類似性検索エンジンです。不均質な計算アーキテクチャーに基づいて効率を最大化出来ます。数十億のベクタの中に目標を検索できるまで数ミリ秒しかかからず、最低限の計算資源だけが必要です。

[基本紹介](https://www.milvus.io/docs/about_milvus/overview.md)を参考して詳しい情報を手に入れます。

Milvus [リリースノート](https://www.milvus.io/docs/v0.6.0/releases/v0.6.0.md)を読んで最新バージョンや更新情報を手に入れます。

# ロードマップ

[ロードマップ](https://github.com/milvus-io/milvus/milestones)を読んでMilvusの将来の特性をわかります。

このロードマップはまだまだ進行中なので、合理的な調整は可能です。全てのコメントや要件や提案などを歓迎です。

## アプリケーション

様々な応用場合があります。MilvusとVGGで構築した画像検索のデモンストレーションです。

[![image retrieval demo](https://raw.githubusercontent.com/milvus-io/docs/v0.7.0/assets/image_retrieval.png)](https://raw.githubusercontent.com/milvus-io/docs/v0.7.0/assets/image_retrieval.png)

ほかの解決方案を見たいと思うなら、[bootcamp](https://github.com/milvus-io/bootcamp)を参考してください。

## テスト報告

[テスト報告](https://github.com/milvus-io/milvus/tree/master/docs)を読んで性能基準をわかります。

## サポートするクライアント

-   [Go](https://github.com/milvus-io/milvus-sdk-go)
-   [Python](https://github.com/milvus-io/pymilvus)
-   [Java](https://github.com/milvus-io/milvus-sdk-java)
-   [C++](https://github.com/milvus-io/milvus/tree/master/sdk)
-   [RESTful API](https://github.com/milvus-io/milvus/tree/master/core/src/server/web_impl)
-   [Node.js](https://www.npmjs.com/package/@arkie-ai/milvus-client) ([arkie](https://www.arkie.cn/)が提供した)

## はじめに

DockerでMilvusをインストールすることは簡単です。[Milvusインストール案内](https://www.milvus.io/docs/guides/get_started/install_milvus/install_milvus.md) を参考してください。ソースからMilvusを構築するために、[ソースから構築する](INSTALL.md)を参考してください。

Milvusをコンフィグするために、[Milvusコンフィグ](https://www.milvus.io/docs/reference/milvus_config.md)を読んでください。

### 初めてのMilvusプログラムを試す

[Python](https://www.milvus.io/docs/guides/get_started/example_code.md)、[Java](https://github.com/milvus-io/milvus-sdk-java/tree/master/examples)、[Go](https://github.com/milvus-io/milvus-sdk-go/tree/master/examples)、または[C++](https://github.com/milvus-io/milvus/tree/master/sdk/examples)などのサンプルコードを使ってMilvusプログラムを試す。

## 貢献規約

本プロジェクトへの貢献に心より感謝いたします。 Milvusを貢献したいと思うなら、[貢献規約](CONTRIBUTING.md)を読んでください。 本プロジェクトはMilvusの[行動規範](CODE_OF_CONDUCT.md)に従います。プロジェクトに参加したい場合は、行動規範を従ってください。

[GitHub issues](https://github.com/milvus-io/milvus/issues) を使って問題やバッグなとを報告しでください。 一般てきな問題なら, Milvusコミュニティに参加してください。

## メーリングリスト

-   [Milvus TSC](https://lists.lfai.foundation/g/milvus-tsc)
-   [Milvus Technical Discuss](https://lists.lfai.foundation/g/milvus-technical-discuss)
-   [Milvus Announce](https://lists.lfai.foundation/g/milvus-announce)

## Milvusコミュニティを参加する

他の貢献者と交流したい場合は、Milvusの [slackチャンネル](https://join.slack.com/t/milvusio/shared_invite/enQtNzY1OTQ0NDI3NjMzLWNmYmM1NmNjOTQ5MGI5NDhhYmRhMGU5M2NhNzhhMDMzY2MzNDdlYjM5ODQ5MmE3ODFlYzU3YjJkNmVlNDQ2ZTk)に参加してください。


## 参考情報

-   [Milvus.io](https://www.milvus.io)

-   [Milvus](https://github.com/milvus-io/bootcamp)

-   [Milvus テストレポート](https://github.com/milvus-io/milvus/tree/master/docs)

-   [Milvus のよくある質問](https://www.milvus.io/docs/faq/operational_faq.md)

-   [Milvus Medium](https://medium.com/@milvusio)

-   [Milvus CSDN](https://zilliz.blog.csdn.net/)

-   [Milvus ツイッター](https://twitter.com/milvusio)

-   [Milvus Facebook](https://www.facebook.com/io.milvus.5)

-   [Milvus デザイン文書](design.md)


## ライセンス

[Apache 2.0ライセンス](LICENSE)
