# Quick start

### Description：

- Test cases can be organized with `yaml `
- Test can run with local mode or helm mode

### Demos：

1. Using jenkins + helm mode： 

2. Local test：
   1. set python path 
   
      `export PYTHONPATH=/your/project/path/milvus_benchmark`
   2. (optional, use `sift` dataset) mount NAS or update `*_DATA_DIR` in `runner.py`
   
        `sudo mount -t cifs -o username=test,vers=1.0 //172.16.70.249/test /test`
   3. run test
   
      `cd milvus-benchmark/`

      `python3 main.py --local --host=*.* --port=19530 --suite=suites/011_cpu_search_debug.yaml`

### Definitions of test suites：

Testers need to write test suite config if adding a customized test into the current test framework

The following are the searching performance test suite：

1. search_performance: the test type，also we have:

    `search_performance`,`build_performance`,`insert_performance`,`accuracy`,`stability`,`search_stability`
2. collections: list of test cases
3. The following fields are in the `collection` field：
   - milvus: milvus config
   - collection_name: currently support one table
   - ni_per: per count of insert
   - index_type: index type
   - index_param: param of index
   - run_count: search count
   - search_params: params of search_vectors
   - top_ks: top_k of search
   - nqs: nq of search

## Test result：

The result of searching performance

Test result will be uploaded, and will be used to judge if the test run pass or failed
