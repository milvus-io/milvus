### Build C++ SDK

The C++ SDK source code is under milvus/sdk.
If you want to build sdk project, follow below steps:
```shell
 # build project
 $ cd [Milvus root path]/sdk
 $ ./build.sh
```

### Try C++ example

Firstly, you need to start a Milvus server.
If you've already built the entire milvus project, just start Milvus server with the following command:
```shell
 # start milvus server
 $ cd [Milvus root path]/core
 $ ./start_server.sh
```
You can also use Docker to start Milvus server:
```shell
 # pull milvus docker image and start milvus server
 $ docker pull milvusdb/milvus:latest
 $ docker run --runtime=nvidia -p 19530:19530 -d milvusdb/milvus:latest
```

Run C++ example:

```shell
 # run milvus C++ example
 $ cd [Milvus root path]/sdk/cmake_build/examples/simple
 $ ./sdk_simple
```

### Make your own C++ client project

Create a folder for the project, and copy C++ SDK header and library files into it.
```shell
 # create project folder
 $ mkdir MyMilvusClient
 $ cd MyMilvusClient
 
 # copy necessary files
 $ cp [Milvus root path]/sdk/cmake_build/libmilvus_sdk.so .
 $ cp [Milvus root path]/sdk/include/MilvusApi.h .
 $ cp [Milvus root path]/sdk/include/Status.h .
```

Create file main.cpp in the project folder, and copy the following code into it:
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

Create file CMakeList.txt in the project folder, and copy the following code into it:
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