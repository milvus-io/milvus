# Quick start

### Description：

This project is used to test performance/reliability/stability for milvus server
- Test cases can be organized with `yaml `
- Test can run with local mode or helm mode

### Demos：

1. Using jenkins + helm mode：

   ![](assets/Parameters.png)

2. Local test：

   `python3 main.py --local --host=*.* --port=19530 --suite=suites/gpu_search_performance_random50m.yaml`

### Definitions of test suites：

Testers need to write test suite config if adding a customizised test into the current test framework

The following are the searching performance test suite：

![](assets/gpu_search_performance_random50m-yaml.png)

1. search_performance: the test type，also we have`build_performance`,`insert_performance`,`accuracy`,`stability`,`search_stability`
2. tables: list of test cases
3. The following fields are in the `table` field：
   - server: run host
   - milvus: config in milvus
   - collection_name: currently support one collection
   - run_count: search count
   - search_params: params of query

## Test result：

The result of searching performance![](assets/milvus-nightly-performance-new-jenkins.png)

Test result will be uploaded if tests run in helm mode, and will be used to judge if the test run pass or failed
