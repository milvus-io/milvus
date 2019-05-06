### Compilation
#### Step 1: install necessery tools

    centos7 : 
        yum install gfortran libsqlite3-dev libsnappy-dev zlib bzip2
        
    ubuntu16.04 : 
        sudo apt-get install gfortran libsqlite3-dev libsnappy-dev zstd bzip2

#### Step 2: build third-parties
Note: If you want to debug into third-parties, you can build debug with CXXFLAGS='-g -O0' with option 
: -t Debug

    cd [sourcecode path]/cpp/thid_party
    ./build.sh -t Debug
    ./build.sh -t Release
    
#### Step 3: build(output to cmake_build folder)
cmake_build/src/vecwise_server is the server

cmake_build/src/libvecwise_engine.a is the static library

    cd [sourcecode path]/cpp
    ./build.sh -t Debug
    ./build.sh -t Release
    
#### To build unittest:
    
    ./build.sh -u
    or
    ./build.sh --unittest
    
    
### Luanch server
Set config in cpp/conf/server_config.yaml

Then luanch server with config:
    
    cd [build output path]
    start_server.sh
    stop_server.sh

### Luanch test_client(only for debug)
If you want to test remote api, you can build test_client.
test_client use same config file with server:
    
    cd [build output path]/test_client
    test_client -c [sourcecode path]/cpp/conf/server_config.yaml

