# Scale Tests
## Goal
Scale tests are designed to check the scalability of Milvus.

For instance, if the dataNode pod expands from one to two:
   - verify the consistency of existing data
   
   - verify that the DDL and DML operation is working

## Prerequisite
   - Kubernetes Cluster
   - Milvus Operator (refer to [Milvus Operator](https://github.com/milvus-io/milvus-operator))

## Test Scenarios
### Milvus in cluster mode
- scale dataNode replicas
   
- expand / shrink indexNode replicas

- scale queryNode replicas

- scale proxy replicas

## How it works

- Milvus scales the number of pods in a deployment based on the milvus operator
  
- Scale test decouple the milvus deployment from the test code
  
- Each test scenario is carried out along the process:
  <br> deploy milvus -> operate milvus -> scale milvus -> verify milvus 
  
- Milvus deployment and milvus scaling are designed in `./customize/milvus_operator.py`

## Run
### Manually
Run a single test scenario manually(take scale dataNode as instance):  
  
- update 
update milvus image tag `IMAGE_TAG` in `scale/constants.py`
  
- run the commands below:  
```bash  
  cd /milvus/tests/python_client/scale  
  
  pytest test_data_node_scale.py::TestDataNodeScale::test_expand_data_node -v -s  
```

### Nightly 
still in planning 
