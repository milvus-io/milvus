# vecwise_engine_server

### Build and Install
Firstly download zdb_server to under same parent folder with zdb_build

    git clone git@192.168.1.105:jinhai/vecwise_engine.git

Install necessery tools

    centos7 : yum install gfortran
    ubunut : sudo apt-install install gfortran

Build third-parties

    cd [sourcecode path]/cpp/thid_party
    ./build.sh
    
Then run build.sh scripts under cpp folder:

    cd [sourcecode path]/cpp
    ./build.sh -t Debug
    ./build.sh -t Release
    
To run unittest:
    
    ./build.sh -u
    or
    ./build.sh --unittest
    
    
### Luanch server
Set config in cpp/conf/server_config.yaml
Then luanch server with config:
    vecwise_engine_server -c [sourcecode path]/cpp/conf/server_config.yaml