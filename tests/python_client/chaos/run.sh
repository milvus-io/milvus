

pods=("standalone" "datacoord" "proxy" "pulsar" "querynode" "rootcoord" "etcd")
for pod in ${pods[*]}
do
echo "run pod kill chaos test for pod $pod "
bash chaos_test.sh $pod pod_kill chaos-test
done

worker_pods=("datanode" "indexnode" "proxy" "querynode")
for pod in ${worker_pods[*]}
do
echo "run pod kill chaos test for pod $pod with 2 replicas"
bash chaos_test.sh $worker_pods pod_kill chaos-test 2

echo "run pod failure chaos test for pod $pod with 2 replicas"
bash chaos_test.sh $worker_pods pod_failure chaos-test 2
done