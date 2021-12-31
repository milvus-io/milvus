#!/bin/bash

# Exit immediately for non zero status
set -e

ns_name=$1
prefix_name=$2
log_dir=${3:-"k8s_logs"}
array=($(kubectl get pod -n ${ns_name}|grep ${prefix_name}|awk '{print $1}'))
echo ${array[@]}
if [ ! -d $log_dir/pod_log ] || [ ! -d $log_dir/pod_describe ];
then
    mkdir -p $log_dir/pod_log
    mkdir -p $log_dir/pod_describe
fi
echo "export logs start"
for pod in ${array[*]}
do
echo "export logs for pod $pod "
kubectl logs $pod -n ${ns_name} > ./$log_dir/pod_log/$pod.log 2>&1
kubectl describe pod $pod -n ${ns_name} > ./$log_dir/pod_describe/$pod.log
done
echo "export logs done"
