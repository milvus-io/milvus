# Chaos Tests
## Goal
Chaos tests are designed to check the reliability of Milvus.

For instance, if one pod is killed:
   - verify that it restarts automatically 
   - verify that the related operation fails, while the other operations keep working successfully during the absence of the pod
   - verify that all the operations work successfully after the pod back to running state
   - verify that no data lost

## Prerequisite
Chaos tests run in pytest framework, same as e2e tests. 

Please refer to [Run E2E Tests](https://github.com/milvus-io/milvus/blob/master/tests/README.md)

## Test Scenarios
### Milvus in cluster mode
#### pod kill
1. root coordinator pod is killed
   
2. proxy pod is killed

3. data coordinator pod is killed

4. data node pod is killed

5. index coordinator pod is killed

6. index node pod is killed

7. query coordinator pod is killed

8. query node pod is killed

9. minio pod is killed
#### pod network partition

two direction(to and from) network isolation between a pod and the rest of the pods

### Milvus in standalone mode
1. standalone pod is killed

2. minio pod is killed

## How it works
- Test scenarios are designed by different chaos objects
- Every chaos object is defined in one yaml file locates in  folder `chaos_objects`
- Every chaos yaml file specified by `ALL_CHAOS_YAMLS` in `constants.py` would be parsed as a parameter and be passed into `test_chaos.py`
- All expectations of every scenario are defined in `testcases.yaml` locates in folder `chaos_objects`
- [Chaos Mesh](https://chaos-mesh.org/) is used to inject chaos into Milvus in `test_chaos.py`

## Run
### Manually
Run a single test scenario manually(take query node pod is killed as instance):
1. update `ALL_CHAOS_YAMLS = 'chaos_querynode_podkill.yaml'` in `constants.py`

2. run the commands below:
   ```bash
   cd /milvus/tests/python_client/chaos

   pytest test_chaos.py --host ${Milvus_IP} -v
   ```
Run multiple test scenario in a category manually(take network partition chaos for all pods as instance):

1. update `ALL_CHAOS_YAMLS = 'chaos_*_network_partition.yaml'` in `constants.py`

2. run the commands below:
   ```bash
   cd /milvus/tests/python_client/chaos

   pytest test_chaos.py --host ${Milvus_IP} -v
   ```
### Github Action
* [Pod Kill Chaos Test](https://github.com/milvus-io/milvus/actions/workflows/pod-kill-chaos-test.yaml)
* [Network Partition Chaos Test](https://github.com/milvus-io/milvus/actions/workflows/network-partition-chaos-test.yaml)

### Nightly 
still in planning 

### Todo
- [ ] pod_failure
- [ ] container_kill
- [x] network attack
- [ ] memory stress

## How to contribute
* Get familiar with chaos engineering and [Chaos Mesh](https://chaos-mesh.org)
* Design chaos scenarios, preferring to pick from todo list
* Generate yaml file for your chaos scenarios. You can create a chaos experiment in chaos-dashboard, then download the yaml file of it.
* Add yaml file to chaos_objects dir and rename it as `chaos_${component_name}_${chaos_type}.yaml`. Make sure `kubectl apply -f ${your_chaos_yaml_file}` can take effect
* Add testcase in `testcases.yaml`. You should figure out the expectation of milvus during the chaos
* Run your added testcase according to `Manually` above and check whether it as your expectation 