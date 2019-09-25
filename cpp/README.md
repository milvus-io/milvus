
- [Slack Community](https://join.slack.com/t/milvusio/shared_invite/enQtNzY1OTQ0NDI3NjMzLWNmYmM1NmNjOTQ5MGI5NDhhYmRhMGU5M2NhNzhhMDMzY2MzNDdlYjM5ODQ5MmE3ODFlYzU3YjJkNmVlNDQ2ZTk)
- [Blog](https://www.milvus.io/blog/)

# Welcome to Milvus

Firstly, welcome, and thanks for your interest in [Milvus](https://milvus.io)! No matter who you are, what you do, we greatly appreciate your contribution to help us reinvent data science with Milvus.

## What is Milvus

Milvus is an open source vector search engine that supports similarity search of large-scale vectors. Built on optimized indexing algorithm, it is compatible with major AI/ML models.

Milvus was developed by ZILLIZ, a tech startup that intends to reinvent data science, with the purpose of providing enterprises with efficient and scalable similarity search and analysis of feature vectors and unstructured data. 

Milvus provides stable Python, C++ and Java API.

Keep up-to-date with newest releases and latest updates by reading [Releases](https://www.milvus-io/docs/master/releases).

- GPU-accelerated search engine

  Milvus is designed for the largest scale of vector index. CPU/GPU heterogeneous computing architecture allows you to process data at a speed 1000 times faster.

- Intelligent index

  With a “Decide Your Own Algorithm” approach, you can embed machine learning and advanced algorithms into Milvus without the headache of complex data engineering or migrating data between disparate systems. Milvus is built on optimized indexing algorithm based on quantization indexing, tree-based and graph indexing methods.

- Strong scalability

  The data is stored and computed on a distributed architecture. This lets you scale data sizes up and down without redesigning the system.

## Architecture
![Milvus_arch](https://www.milvus-io/docs/master/assets/milvus_arch.png)

## Get started

### Install and start Milvus server

#### Use Docker

Use Docker to install Milvus is a breeze. See the [Milvus install guide](https://milvus.io/docs/en/userguide/install_milvus/) for details.

#### Use source code

##### Compilation

###### Step 1 Install necessary tools

```shell
# Install tools
Centos7 : 
$ yum install gfortran qt4 flex bison mysql-devel mysql
    
Ubuntu16.04 : 
$ sudo apt-get install gfortran qt4-qmake flex bison libmysqlclient-dev mysql-client
       
```

Verify the existence of `libmysqlclient_r.so`:

```shell
# Verify existence
$ locate libmysqlclient_r.so
```

If not, you need to create a symbolic link:

```shell
# Create symbolic link
$ sudo ln -s /path/to/libmysqlclient.so /path/to/libmysqlclient_r.so
```

###### Step 2 Build

```shell
TBD
cd [Milvus sourcecode path]/cpp/thirdparty
git clone git@192.168.1.105:megasearch/knowhere.git
cd knowhere
./build.sh -t Debug
or ./build.sh -t Release

cd [sourcecode path]/cpp
./build.sh -t Debug
or ./build.sh -t Release
```

When the build is completed, all the stuff that you need in order to run Milvus will be installed under `[Milvus root path]/cpp/milvus`.

If you encounter the following error message,
`protocol https not supported or disabled in libcurl`

please reinstall CMake with curl:

1. Install curl development files:
   ```shell
   CentOS 7:   
   $ yum install curl-devel
   Ubuntu 16.04: 
   $ sudo apt-get install libcurl4-openssl-dev
   ```

2. Install [CMake 3.14](https://github.com/Kitware/CMake/releases/download/v3.14.6/cmake-3.14.6.tar.gz): 
   ```shell
   $ ./bootstrap --system-curl 
   $ make 
   $ sudo make install
   ```

##### Run unit test

```shell
$ ./build.sh -u
or
$ ./build.sh --unittest
```

##### Run code coverage

```shell
CentOS 7:   
$ yum install lcov
Ubuntu 16.04: 
$ sudo apt-get install lcov
    
$ ./build.sh -u -c
```

##### Launch Milvus server

```shell
$ cd [Milvus root path]/cpp/milvus
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

Create a new file `example.py`, and add [Python example code](https://github.com/milvus-io/pymilvus/blob/branch-0.3.1/examples/AdvancedExample.py) to it.

Run the example code.

```python
# Run Milvus Python example
$ python3 example.py
```

#### Run C++ example code

```shell
 # Run Milvus C++ example
 $ cd [Milvus root path]/cpp/milvus/bin
 $ ./sdk_simple
```

## Contribution guidelines

Contributions are welcomed and greatly appreciated. If you want to contribute to Milvus, please read the [contribution guidelines](CONTRIBUTING.md). This project adheres to the [code of conduct](CODE OF CONDUCT.md) of Milvus. By participating, you are expected to uphold this code.

We use [GitHub issues](https://github.com/milvus-io/milvus/issues) to track issues and bugs. For general questions and discussions, please go to [Milvus Forum]. 

## Join the Milvus community

For public discussion of Milvus, please join our [discussion group](milvusio.slack.com). 

## Milvus Roadmap

Please read our [roadmap](milvus-io/milvus/docs/master/roadmap.md) to learn about upcoming features.

## Resources

[Milvus official website](https://www.milvus.io)

[Milvus docs](https://www.milvus.io/docs/en/QuickStart/)

[Milvus blog](https://www.milvus.io/blog/)

[Milvus CSDN](https://mp.csdn.net/mdeditor/100041006#)

[Milvus roadmap](https://www.milvus-io/docs/master/roadmap.md)


## License

[Apache 2.0 license](milvus-io/milvus/LICENSE.md)