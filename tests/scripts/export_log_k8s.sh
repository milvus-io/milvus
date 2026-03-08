#!/bin/bash

# Exit immediately for non zero status
set -e

ns_name=$1
instance_name=$2
log_dir=${3:-"k8s_logs"}

#show proxy pod log
array=($(kubectl get pod -n ${ns_name} -l "component=proxy, app.kubernetes.io/instance=${instance_name}"| awk 'NR == 1 {next} {print $1}'))
echo ${array[@]}

for pod in ${array[*]}
do
echo "show log of proxy pod $pod "
kubectl logs $pod -n ${ns_name} --tail=100 || echo "show log for pod $pod failed"
done

# export info of etcd
array=($(kubectl get pod -n ${ns_name} -l "app.kubernetes.io/name=etcd, app.kubernetes.io/instance=${instance_name}"| awk 'NR == 1 {next} {print $1}'))
echo ${array[@]}
mkdir -p $log_dir/etcd_session
for pod in ${array[*]}
do
echo "check session for etcd pod $pod "
kubectl exec $pod -n ${ns_name} -- etcdctl get --prefix by-dev/meta/session > ./$log_dir/etcd_session/$pod.log || echo "export session for pod $pod failed"
done
echo "check session done"

# export logs of all pods
array_1=($(kubectl get pod -n ${ns_name} -l "app.kubernetes.io/instance=${instance_name}"| awk 'NR == 1 {next} {print $1}'))
array_2=($(kubectl get pod -n ${ns_name} -l "release=${instance_name}"| awk 'NR == 1 {next} {print $1}'))
array=(${array_1[@]} ${array_2[@]})

echo ${array[@]}
if [ ! -d $log_dir/pod_log ] || [ ! -d $log_dir/pod_describe ];
then
    mkdir -p $log_dir/pod_log
    mkdir -p $log_dir/pod_log_previous
    mkdir -p $log_dir/pod_describe
fi
echo "export logs start"
for pod in ${array[*]}
do
echo "export logs for pod $pod "
kubectl logs $pod -n ${ns_name} > ./$log_dir/pod_log/$pod.log 2>&1 || echo "export log for pod $pod failed"
kubectl logs $pod --previous -n ${ns_name} > ./$log_dir/pod_log_previous/$pod.log 2>&1 || echo "pod $pod has no previous log"
kubectl describe pod $pod -n ${ns_name} > ./$log_dir/pod_describe/$pod.log 2>&1 || echo "describe pod $pod failed"
done
echo "export logs done"

# export goroutine of all milvus pods
array=($(kubectl get pod -n ${ns_name} -l "app.kubernetes.io/instance=${instance_name}, app.kubernetes.io/name=milvus"| awk 'NR == 1 {next} {print $1}'))

echo ${array[@]}
if [ ! -d $log_dir/goroutine ];
then
    mkdir -p $log_dir/goroutine
fi
echo "export goroutine start"
for pod in ${array[*]}
do
echo "export goroutine for pod $pod "
ip=($(kubectl get pod ${pod} -n ${ns_name} -o jsonpath='{.status.podIP}'))
echo $ip
curl "http://${ip}:9091/debug/pprof/goroutine?debug=10" -o ./$log_dir/goroutine/$pod.log || echo "export goroutine for pod $pod failed"
done
echo "export goroutine done"
