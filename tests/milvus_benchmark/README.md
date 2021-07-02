# Quick start

### Description：

- Test suites can be organized with `yaml `
- Test can run with local mode or argo/jenkins mode, that manage the server env in argo/jenkins step or stages

### Demos：

1. Using argo pipeline： 
   Run test suites(1.x, 2.x version) in argo workflows, innernal argo url: argo-test.zilliz.cc

2. Local test：
   Run test with the local server
   1. set python path 
   
      `export PYTHONPATH=/yourmilvusprojectpath/tests/milvus_benchmark`
   2. (optional, for `sift`/`glove` open dataset) mount NAS or update `*_DATA_DIR` in `runner.py`
   
        `sudo mount -t cifs -o username=test,vers=1.0 //172.16.70.249/test /test`
   3. run test
   
      `cd milvus-benchmark/`

      `python3 main.py --local --host=*.* --port=19530 --suite=suites/2_insert_data.yaml`

### Definitions of test suites：

Testers need to write test suite config if adding a customized test into the current test framework

The following are the searching performance test suite：

1. insert_search_performance: the test type，also we have:

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

Test result will be uploaded, which will be used to tell if the test run pass or failed
