![Milvuslogo](https://milvus.io/docs/assets/milvus_logo.png)

- [Slack Community](https://join.slack.com/t/milvusio/shared_invite/enQtNzY1OTQ0NDI3NjMzLWNmYmM1NmNjOTQ5MGI5NDhhYmRhMGU5M2NhNzhhMDMzY2MzNDdlYjM5ODQ5MmE3ODFlYzU3YjJkNmVlNDQ2ZTk)
- [Blog](https://www.milvus.io/blog/)
- [Twitter](https://twitter.com/milvus_io)

# Welcome to Milvus

Firstly, welcome, and thanks for your interest in [Milvus](https://milvus.io)! No matter who you are, what you do, we greatly appreciate your contribution to help us reinvent data science with Milvus.

## What is Milvus

Milvus is an open source vector search engine that supports similarity search of large-scale vectors. Built on optimized indexing algorithm, it is compatible with major AI/ML models.

Milvus was developed by ZILLIZ, a tech startup that intends to reinvent data science, with the purpose of providing enterprises with efficient and scalable similarity search and analysis of feature vectors and unstructured data. 

Milvus provides stable Python, C++ and Java APIs.

Keep up-to-date with newest releases and latest updates by reading Milvus [release notes](https://milvus.io/docs/en/Releases/v0.4.0/).

- GPU-accelerated search engine

  Milvus is designed for the largest scale of vector index. CPU/GPU heterogeneous computing architecture allows you to process data at a speed 1000 times faster.

- Intelligent index

  With a “Decide Your Own Algorithm” approach, you can embed machine learning and advanced algorithms into Milvus without the headache of complex data engineering or migrating data between disparate systems. Milvus is built on optimized indexing algorithm based on quantization indexing, tree-based and graph indexing methods.

- Strong scalability

  The data is stored and computed on a distributed architecture. This lets you scale data sizes up and down without redesigning the system.

## Architecture
![Milvus_arch](https://milvus.io/docs/assets/milvus_arch.png)

## Get started

### Install using docker

Use Docker to install Milvus is a breeze. See the [Milvus install guide](https://milvus.io/docs/en/userguide/install_milvus/) for details.

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
./ubuntu_build_deps.sh
```

##### Step 2 Build

```shell
$ cd [Milvus sourcecode path]/core
$ ./build.sh -t Debug
or 
$ ./build.sh -t Release
```

When the build is completed, all the stuff that you need in order to run Milvus will be installed under `[Milvus root path]/core/milvus`.

#### Code format and linting

Install clang-format
```shell
$ sudo apt-get install clang-format
$ rm cmake_build/CMakeCache.txt
```
Check code style
```shell
$ ./build.sh -l
```
To format the code
```shell
$ cd cmake_build
$ make clang-format
```

#### Run unit test

```shell
$ ./build.sh -u
```

#### Run code coverage

Install lcov
```shell
$ sudo apt-get install lcov
```
```shell  
$ ./build.sh -u -c
```
Run MySQL docker
```shell 
docker pull mysql:latest
docker run -p 3306:3306 -e MYSQL_ROOT_PASSWORD=123456 -d mysql:latest
```
Run code coverage

```shell  
$ ./coverage.sh -u root -p 123456 -t 127.0.0.1
```

Or start your own MySQL server, and then run code coverage

```shell
$ ./coverage.sh -u ${MYSQL_USERNAME} -p ${MYSQL_PASSWORD} -t ${MYSQL_SERVER_IP}
```

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

To edit Milvus settings in `conf/server_config.yaml` and `conf/log_config.conf`, please read [Milvus Configuration](https://www.milvus-io/docs/master/reference/milvus_config.md).

### Try your first Milvus program

#### Run Python example code

Make sure [Python 3.4](https://www.python.org/downloads/) or higher is already installed and in use.

Install Milvus Python SDK.

```shell
# Install Milvus Python SDK
$ pip install pymilvus==0.2.0
```

Create a new file `example.py`, and add [Python example code](https://github.com/milvus-io/pymilvus/blob/master/examples/AdvancedExample.py) to it.

Run the example code.

```python
# Run Milvus Python example
$ python3 example.py
```

#### Run C++ example code

```shell
 # Run Milvus C++ example
 $ cd [Milvus root path]/core/milvus/bin
 $ ./sdk_simple
```

## Contribution guidelines

Contributions are welcomed and greatly appreciated. If you want to contribute to Milvus, please read our [contribution guidelines](CONTRIBUTING.md). This project adheres to the [code of conduct](CODE_OF_CONDUCT.md) of Milvus. By participating, you are expected to uphold this code.

We use [GitHub issues](https://github.com/milvus-io/milvus/issues/new) to track issues and bugs. For general questions and public discussions, please join our community.

## Join the Milvus community

To connect with other users and contributors, welcome to join our [slack channel](https://join.slack.com/t/milvusio/shared_invite/enQtNzY1OTQ0NDI3NjMzLWNmYmM1NmNjOTQ5MGI5NDhhYmRhMGU5M2NhNzhhMDMzY2MzNDdlYjM5ODQ5MmE3ODFlYzU3YjJkNmVlNDQ2ZTk). 

## Milvus Roadmap

Please read our [roadmap](https://milvus.io/docs/en/roadmap/) to learn about upcoming features.

## Resources

[Milvus official website](https://www.milvus.io)

[Milvus docs](https://www.milvus.io/docs/en/userguide/install_milvus/)

[Milvus blog](https://www.milvus.io/blog/)

[Milvus CSDN](https://mp.csdn.net/mdeditor/100041006#)

[Milvus roadmap](https://milvus.io/docs/en/roadmap/)


## License

[Apache 2.0 license](milvus-io/milvus/LICENSE.md)
