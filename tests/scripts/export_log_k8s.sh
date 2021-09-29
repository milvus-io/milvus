#!/bin/bash
set -e

ns_name=$1
prefix_name=$2
array=($(kubectl get pod -n ${ns_name}|grep ${prefix_name}|awk '{print $1}'))
echo ${array[@]}
log_dir="k8s_logs"
if [ ! -d $log_dir  ];
then
    mkdir $log_dir
fi
echo "export logs start"
for pod in ${array[*]}
do
echo "export logs for pod $pod "
kubectl logs $pod -n ${ns_name} > ./$log_dir/$pod.log
done
echo "export logs done"
