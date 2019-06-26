### Compilation
#### Step 1: install necessery tools

    centos7 : 
        yum install gfortran flex bison
        
    ubuntu16.04 : 
        sudo apt-get install gfortran flex bison

#### Step 2: build(output to cmake_build folder)
cmake_build/src/milvus_server is the server

cmake_build/src/libmilvus_engine.a is the static library

    git submodule init
    git submodule update
    
    cd [sourcecode path]/cpp
    ./build.sh -t Debug
    ./build.sh -t Release
    ./build.sh -l -t Release # Build license version(only available for Release)

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