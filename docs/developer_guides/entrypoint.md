## Entrypoint

Usage:

- Command line arguments

  - ```shell
    $ ./milvus-distributed --help
    usage: main [<flags>]
    
    Milvus
    
    Flags:
          --help               Show context-sensitive help (also try --help-long and --help-man).
      -m, --master             Run master service
      -M, --msgstream-service  Run msgstream service
      -p, --proxy-service      Run proxy service
      -P, --proxy-node         Run proxy node
      -q, --query-service      Run query service
      -Q, --query-node         Run query node
      -d, --data-service       Run data service
      -D, --data-node          Run data node
      -i, --index-service      Run index service
      -I, --index-node         Run index node
    
    
    # Startup master and proxy in a container
    $ ./milvus-distributed --master --proxy
    ```

- environment variables

  - ```
    $ export ENABLE_MASTER=1
    $ export ENABLE_PROXY=1
    $ ./milvus-distributed
    ```

  - ```shell
    $ ENABLE_MASTER=1 ENABLE_PROXY=1 ./milvus-distributed
    ```

- docker-compose

  - ```yaml
      milvus-master:
        image: milvusdb/milvus-distributed:latest
        environment: 
          - master=1
      
      milvus-proxy:
        image: milvusdb/milvus-distributed:latest
        environment: 
          - proxy=1
    ```

    

