### Compilation
#### Step 1: install necessery tools

    centos7 : 
        yum install gfortran qt4 flex bison mysql-devel mysql
        
    ubuntu16.04 : 
        sudo apt-get install gfortran qt4-qmake flex bison libmysqlclient-dev mysql-client
        
    cd scripts && sudo ./requirements.sh     

If `libmysqlclient_r.so` does not exist after installing MySQL Development Files, you need to create a symbolic link:

```
sudo ln -s /path/to/libmysqlclient.so /path/to/libmysqlclient_r.so
```

#### Step 2: build(output to cmake_build folder)

cmake_build/src/milvus_server is the server

    cd [sourcecode path]/cpp/thirdparty
    git clone git@192.168.1.105:megasearch/knowhere.git
    cd knowhere
    ./build.sh -t Debug
    or ./build.sh -t Release

    cd [sourcecode path]/cpp
    ./build.sh -t Debug
    or ./build.sh -t Release

If you encounter the following error when building:
`protocol https not supported or disabled in libcurl`

1. Install libcurl4-openssl-dev

2. Install cmake 3.14: 

   ```
   ./bootstrap --system-curl 
   make 
   sudo make install
   ```

#### To build unittest:

    ./build.sh -u
    or
    ./build.sh --unittest

#### To run code coverage

    apt-get install lcov
    ./build.sh -u -c

### Launch server
Set config in cpp/conf/server_config.yaml

Add milvus/lib to LD_LIBRARY_PATH

```
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/path/to/milvus/lib
```

Then launch server with config:
    cd [build output path]
    start_server.sh
    stop_server.sh

### Launch test_client(only for debug)
If you want to test remote api, you can run sdk example.
    [build output path]/sdk/examples/grpcsimple/sdk_simple
