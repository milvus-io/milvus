### Compilation
#### Step 1: install necessery tools

    centos7 : 
        yum install gfortran libsqlite3-dev libsnappy-dev libzstd-dev bzip2
        
    ubuntu16.04 : 
        sudo apt-get install gfortran libsqlite3-dev libsnappy-dev libzstd-dev bzip2 liblz4-dev

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
    ./build.sh -g # Build GPU version
    
#### To build unittest:
    
    ./build.sh -u
    or
    ./build.sh --unittest
    
    
### Launch server
Set config in cpp/conf/server_config.yaml

Then launch server with config:
    
    cd [build output path]
    start_server.sh
    stop_server.sh

### Launch test_client(only for debug)
If you want to test remote api, you can build test_client.
test_client use same config file with server:
    
    cd [build output path]/test_client
    test_client -c [sourcecode path]/cpp/conf/server_config.yaml

### License Generate
Use get_sys_info to get system info file.

    ./get_sys_info                  # system.info will be generated

Use license_generator to generate license file.

    ./license_generator -s system.info -l system.license -b 2019-05-15 -e 2019-08-14                 

Copy the license file to path assigned by license_path in server config file.