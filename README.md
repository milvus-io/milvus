
![Milvuslogo](https://github.com/milvus-io/docs/blob/master/assets/milvus_logo.png)


![LICENSE](https://img.shields.io/badge/license-Apache--2.0-brightgreen)
![Language](https://img.shields.io/badge/language-C%2B%2B-blue)
[![codebeat badge](https://codebeat.co/badges/e030a4f6-b126-4475-a938-4723d54ec3a7?style=plastic)](https://codebeat.co/projects/github-com-jinhai-cn-milvus-master)
![Release](https://img.shields.io/badge/release-v0.5.0-orange)
![Release_date](https://img.shields.io/badge/release_date-October-yellowgreen)

- [Slack Community](https://join.slack.com/t/milvusio/shared_invite/enQtNzY1OTQ0NDI3NjMzLWNmYmM1NmNjOTQ5MGI5NDhhYmRhMGU5M2NhNzhhMDMzY2MzNDdlYjM5ODQ5MmE3ODFlYzU3YjJkNmVlNDQ2ZTk)
- [Twitter](https://twitter.com/milvusio)
- [Facebook](https://www.facebook.com/io.milvus.5)
- [Blog](https://www.milvus.io/blog/)
- [CSDN](https://zilliz.blog.csdn.net/)
- [中文官网](https://www.milvus.io/zh-CN/)


## What is Milvus

Milvus is an open source similarity search engine for massive-scale feature vectors. Built with heterogeneous computing architecture for the best cost efficiency. Searches over billion-scale vectors take only milliseconds with minimum computing resources.

For more detailed introduction of Milvus and its architecture, see [Milvus overview](https://www.milvus.io/docs/en/aboutmilvus/overview/).

Milvus provides stable [Python](https://github.com/milvus-io/pymilvus), [Java](https://milvus-io.github.io/milvus-sdk-java/javadoc/io/milvus/client/package-summary.html) and C++ APIs. 

Keep up-to-date with newest releases and latest updates by reading Milvus [release notes](https://www.milvus.io/docs/en/release/v0.5.0/).

## Get started

### Hardware requirements

| Component | Recommended configuration           |
| --------- | ----------------------------------- |
| CPU       | Intel CPU Haswell or higher         |
| GPU       | NVIDIA Pascal series or higher      |
| RAM       | 8 GB or more (depends on data size) |
| Hard drive| SATA 3.0 SSD or higher              |

### Install using docker

Using Docker to install Milvus is a breeze. See the [Milvus install guide](https://milvus.io/docs/en/userguide/install_milvus/) for details.

### Build from source

#### Software requirements

- Ubuntu 18.04 or higher
- CMake 3.14 or higher
- CUDA 10.0 or higher
- NVIDIA driver 418 or higher

#### Compilation

##### Step 1 Install dependencies

```shell
$ cd [Milvus sourcecode path]/core
$ ./ubuntu_build_deps.sh
```

##### Step 2 Build

```shell
$ cd [Milvus sourcecode path]/core
$ ./build.sh -t Debug
or 
$ ./build.sh -t Release
```

When the build is completed, all the stuff that you need in order to run Milvus will be installed under `[Milvus root path]/core/milvus`.

#### Launch Milvus server

```shell
$ cd [Milvus root path]/core/milvus
```

Add `lib/` directory to `LD_LIBRARY_PATH`

```
$ export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/path/to/milvus/lib
```

Then start Milvus server:

```
$ cd scripts
$ ./start_server.sh
```

To stop Milvus server, run:

```shell
$ ./stop_server.sh
```

To edit Milvus settings in `conf/server_config.yaml` and `conf/log_config.conf`, please read [Milvus Configuration](https://github.com/milvus-io/docs/blob/master/reference/milvus_config.md).

### Try your first Milvus program

#### Run Python example code

Please read [this page](https://www.milvus.io/docs/en/userguide/example_code/) for how to run an example program using Python SDK.

#### Run Java example code

Make sure Java 8 or higher is already installed.

Refer to [this link](https://github.com/milvus-io/milvus-sdk-java/tree/master/examples) for the example code.

#### Run C++ example code

```shell
 # Run Milvus C++ example
 $ cd [Milvus root path]/core/milvus/bin
 $ ./sdk_simple
```

## Roadmap

Please read our [roadmap](https://milvus.io/docs/en/roadmap/) to learn about upcoming features.

## Contribution guidelines

Contributions are welcomed and greatly appreciated. Please read our [contribution guidelines](CONTRIBUTING.md) for detailed contribution workflow. This project adheres to the [code of conduct](CODE_OF_CONDUCT.md) of Milvus. By participating, you are expected to uphold this code.

We use [GitHub issues](https://github.com/milvus-io/milvus/issues/new/choose) to track issues and bugs. For general questions and public discussions, please join our community.

## Join our community

To connect with other users and contributors, welcome to join our [slack channel](https://join.slack.com/t/milvusio/shared_invite/enQtNzY1OTQ0NDI3NjMzLWNmYmM1NmNjOTQ5MGI5NDhhYmRhMGU5M2NhNzhhMDMzY2MzNDdlYjM5ODQ5MmE3ODFlYzU3YjJkNmVlNDQ2ZTk). 

## Thanks

We greatly appreciate the help of the following people.

- [akihoni](https://github.com/akihoni) found a broken link and a small typo in the README file.

## Resources

[Milvus official website](https://www.milvus.io)

[Milvus docs](https://www.milvus.io/docs/en/userguide/install_milvus/)

[Milvus bootcamp](https://github.com/milvus-io/bootcamp)

[Milvus blog](https://www.milvus.io/blog/)

[Milvus CSDN](https://zilliz.blog.csdn.net/)

[Milvus roadmap](https://milvus.io/docs/en/roadmap/)

## License

[Apache License 2.0](LICENSE)


