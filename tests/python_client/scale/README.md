# Scale Tests
## Goal
Scale tests are designed to check the scalability of Milvus.

For instance, if the dataNode pod expands from one to two:
   - verify the consistency of existing data
   
   - verify that the DDL and DML operation is working

## Prerequisite
   - Milvus Helm Chart ( refer to [Milvus Helm Chart](https://github.com/milvus-io/milvus-helm/blob/master/charts/milvus/README.md) )

## Test Scenarios
### Milvus in cluster mode
- expand / shrink dataNode pod
   
- expand / shrink indexNode pod

- expand / shrink queryNode pod

- expand / shrink proxy pod

## How it works

- Milvus scales the number of pods in a deployment based on the helm upgrade
  
- Scale test decouple the milvus deployment from the test code
  
- Each test scenario is carried out along the process:
  <br> deploy milvus -> operate milvus -> scale milvus -> verify milvus 
  
- Milvus deployment and milvus scaling are designed in `helm_env.py`

## Run
### Manually
Run a single test scenario manually(take scale dataNode as instance):  
  
- update milvus helm chart path
```bash  
  export MILVUS_CHART_ENV=/your/milvus-helm/charts/milvus  
``` 
  
- run the commands below:  
```bash  
  cd /milvus/tests/python_client/scale  
  
  pytest test_data_node_scale.py::TestDataNodeScale::test_expand_data_node -v -s  
```

### Nightly 
still in planning 
