`milvus_benchmark` is a non-functional testing tool which allows users to run tests on k8s cluster or at local, the primary use case is performance/load/stability testing, the objective is to expose problems in milvus project.

## Quick start

### Description：

- Test cases in `milvus_benchmark` can be organized with `yaml`
- Test can run with local mode or helm mode
   - local: install and start your local server, and pass the host/port param when start the tests
   - helm: install the server by helm, which will manage the milvus in k8s cluster, and you can interagte the test stage into argo workflow or jenkins pipeline

### Usage：

1. Using jenkins:
   Use `ci/main_jenkinsfile` as the jenkins pipeline file
2. Using argo： 
   example argo workflow yaml configuration: `ci/argo.yaml`
3. Local test：

   1). set PYTHONPATH:
   
      `export PYTHONPATH=/your/project/path/milvus_benchmark`
   
   2). prepare data: 
   
      if we need to use the sift/deep dataset as the raw data input, we need to mount NAS and update `RAW_DATA_DIR` in `config.py`, the example mount command:
        `sudo mount -t cifs -o username=test,vers=1.0 //172.16.70.249/test /test`
   
   3). install requirements:

      `pip install -r requirements.txt`
   
   4). write test yaml and run with the yaml param:
   
      `cd milvus-benchmark/ && python main.py --local --host=* --port=19530 --suite=suites/2_insert_data.yaml`

### Definitions of test suite：

Test suite yaml defines the test process, users need to write test suite yaml if adding a customized test into the current test framework.

Take the test file `2_insert_data.yaml` as an example, the top level is the test type: `insert_performance`, there are lots of test types including: `search_performance/build_performance/insert_performance/accuracy/locust_insert/...`, each test type corresponds to this different runner defined in directory `runnners`, the other parts in the test yaml is the params pass to the runner, such as the field `collection_name` means which kind of collection will be created in milvus.

### Test result：

Test result will be uploaded if run with the helm mode, which will be used to judge if the test run pass or failed.
