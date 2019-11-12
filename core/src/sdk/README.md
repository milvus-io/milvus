### Build C++ sdk

The C++ sdk source code is under milvus/core/src/sdk. Build entire milvus project will also build the sdk project. 
If you don't want to build entire milvus project, you can do the following steps:
```shell
 # generate make files
 $ cd [Milvus root path]/core
 $ ./build.sh -l
 
 # build C++ sdk project
 $ cd [Milvus root path]/core/cmake_build
 $ make -C src/sdk
```

### Try C++ example

Firstly you need to launch a milvus server.
If you already build entire milvus project, just run:
```shell
 # start milvus server
 $ cd [Milvus root path]/core
 $ ./start_server.sh
```
You also can pull milvus release docker image to launch milvus server:
```shell
 # pull milvus docker image and start milvus server
 $ docker pull milvusdb/milvus:latest
 $ docker run --runtime=nvidia -p 19530:19530 -d milvusdb/milvus:latest
```

Run C++ example:

```shell
 # run milvus C++ example
 $ cd [Milvus root path]/core/cmake_build/src/sdk/examples/simple
 $ ./sdk_simple
```

### Make your own C++ client project

Firstly create a project folder. And copy C++ sdk header and library files into the folder.
```shell
 # create project folder
 $ mkdir MyMilvusClient
 $ cd MyMilvusClient
 
 # copy necessary files
 $ cp [Milvus root path]/core/cmake_build/src/sdk/libmilvus_sdk.so .
 $ cp [Milvus root path]/core/src/sdk/include/MilvusApi.h .
 $ cp [Milvus root path]/core/src/sdk/include/Status.h .
```

Create main.cpp under the project folder, and paste the following code into the file:
```shell
#include "./MilvusApi.h"
#include "./Status.h"

int main() {
  // connect to milvus server
  std::shared_ptr<milvus::Connection> conn = milvus::Connection::Create();
  milvus::ConnectParam param = {"127.0.0.1", "19530"};
  conn->Connect(param);
  
  // put your client code here
  
  milvus::Connection::Destroy(conn);
  return 0;
}
```

Create CMakeList.txt under the project folder, and paste the following code into the file:
```shell
 cmake_minimum_required(VERSION 3.14)
 project(test)
 set(CMAKE_CXX_STANDARD 14)

 add_executable(milvus_client main.cpp)
 target_link_libraries(milvus_client
         ${PROJECT_SOURCE_DIR}/libmilvus_sdk.so)
```

Now there are 5 files in your project:
```shell
MyMilvusClient
 |-CMakeList.txt
 |-main.cpp
 |-libmilvus_sdk.so
 |-MilvusApi.h
 |-Status.h
  ```

Build the project:
```shell
 $ mkdir cmake_build
 $ cd cmake_build
 $ cmake ..
 $ make
```

Run your client program:
```shell
 $ ./milvus_client
```