### Compilation
#### Step 1: install necessery tools

    centos7 : yum install gfortran
    ubuntu16.04 : sudo apt-install install gfortran libsqlite3-dev

#### Step 2: build third-parties

    cd [sourcecode path]/cpp/thid_party
    ./build.sh
    
#### Step 3: build(output to cmake_build folder)
cmake_build/src/vecwise_engine_server is the server

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
    
    vecwise_engine_server -c [sourcecode path]/cpp/conf/server_config.yaml

