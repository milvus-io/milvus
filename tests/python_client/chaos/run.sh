

pods=("standalone" "datacoord" "proxy" "pulsar" "querynode" "rootcoord" "etcd")
for pod in ${pods[*]}
do
echo "run pod kill chaos test for pod $pod "
bash chaos_test.sh $pod
done